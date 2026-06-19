from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ── Default arguments ────────────────────────────────────────────
default_args = {
    'owner': 'dinesh_reddy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG definition ───────────────────────────────────────────────
dag = DAG(
    dag_id='ipl_analytics_pipeline',
    default_args=default_args,
    description='End-to-end IPL data pipeline: CSV -> PostgreSQL star schema',
    schedule_interval='0 6 * * *',  # Runs daily at 6:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ipl', 'cricket', 'data-engineering'],
)

# ── Database connection ──────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ipl_db",
        user="ipl_user",
        password="ipl_pass"
    )

# ── Task 1: Validate source files ────────────────────────────────
def validate_source_files(**kwargs):
    import os
    files = ['data/matches.csv', 'data/deliveries.csv']
    for f in files:
        if not os.path.exists(f):
            raise FileNotFoundError(f"Source file missing: {f}")
        size = os.path.getsize(f)
        if size < 1000:
            raise ValueError(f"File too small, may be corrupt: {f}")
        print(f"✅ Validated: {f} ({size:,} bytes)")

# ── Task 2: Extract data ─────────────────────────────────────────
def extract_data(**kwargs):
    matches = pd.read_csv('data/matches.csv')
    deliveries = pd.read_csv('data/deliveries.csv')

    # Data quality checks
    assert len(matches) > 0, "matches.csv is empty"
    assert len(deliveries) > 0, "deliveries.csv is empty"
    assert 'id' in matches.columns, "matches.csv missing 'id' column"
    assert 'match_id' in deliveries.columns, "deliveries.csv missing 'match_id' column"

    print(f"✅ Extracted {len(matches)} matches and {len(deliveries)} deliveries")

    # Push to XCom for next task
    kwargs['ti'].xcom_push(key='match_count', value=len(matches))
    kwargs['ti'].xcom_push(key='delivery_count', value=len(deliveries))

# ── Task 3: Load dimension tables ────────────────────────────────
def load_dimensions(**kwargs):
    matches = pd.read_csv('data/matches.csv')
    deliveries = pd.read_csv('data/deliveries.csv')
    conn = get_conn()
    cur = conn.cursor()

    # dim_teams
    teams = set(matches['team1'].dropna()) | set(matches['team2'].dropna())
    for team in sorted(teams):
        cur.execute("""
            INSERT INTO dim_teams (team_name)
            VALUES (%s) ON CONFLICT (team_name) DO NOTHING
        """, (team,))
    print(f"✅ dim_teams: {len(teams)} teams")

    # dim_players
    players = (
        set(deliveries['batter'].dropna()) |
        set(deliveries['bowler'].dropna()) |
        set(matches['player_of_match'].dropna())
    )
    for player in sorted(players):
        cur.execute("""
            INSERT INTO dim_players (player_name)
            VALUES (%s) ON CONFLICT (player_name) DO NOTHING
        """, (player,))
    print(f"✅ dim_players: {len(players)} players")

    # dim_venues
    venues = matches[['venue', 'city']].drop_duplicates().dropna(subset=['venue'])
    for _, row in venues.iterrows():
        cur.execute("""
            INSERT INTO dim_venues (venue_name, city)
            VALUES (%s, %s) ON CONFLICT (venue_name) DO NOTHING
        """, (row['venue'], row.get('city')))
    print(f"✅ dim_venues: {len(venues)} venues")

    # dim_matches
    cur.execute("SELECT team_name, team_id FROM dim_teams")
    teams_map = dict(cur.fetchall())
    cur.execute("SELECT player_name, player_id FROM dim_players")
    players_map = dict(cur.fetchall())
    cur.execute("SELECT venue_name, venue_id FROM dim_venues")
    venues_map = dict(cur.fetchall())

    def parse_season(val):
        if pd.isna(val):
            return None
        val = str(val).strip()
        if '/' in val:
            return int(val.split('/')[0])
        try:
            return int(val)
        except:
            return None

    loaded = 0
    for _, row in matches.iterrows():
        try:
            cur.execute("""
                INSERT INTO dim_matches (
                    match_id, season, match_date, venue_id,
                    team1_id, team2_id, toss_winner_id, toss_decision,
                    winner_id, player_of_match_id, result_margin
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (match_id) DO NOTHING
            """, (
                int(row['id']),
                parse_season(row['season']),
                row['date'] if pd.notna(row['date']) else None,
                venues_map.get(row['venue']),
                teams_map.get(row['team1']),
                teams_map.get(row['team2']),
                teams_map.get(row['toss_winner']),
                row['toss_decision'] if pd.notna(row['toss_decision']) else None,
                teams_map.get(row['winner']) if pd.notna(row.get('winner')) else None,
                players_map.get(row['player_of_match']) if pd.notna(row.get('player_of_match')) else None,
                float(row['result_margin']) if pd.notna(row.get('result_margin')) else None
            ))
            loaded += 1
        except Exception as e:
            conn.rollback()
    print(f"✅ dim_matches: {loaded} matches")

    conn.commit()
    cur.close()
    conn.close()

# ── Task 4: Load fact table ──────────────────────────────────────
def load_facts(**kwargs):
    deliveries = pd.read_csv('data/deliveries.csv')
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT team_name, team_id FROM dim_teams")
    teams_map = dict(cur.fetchall())
    cur.execute("SELECT player_name, player_id FROM dim_players")
    players_map = dict(cur.fetchall())
    cur.execute("SELECT match_id FROM dim_matches")
    valid_match_ids = set(row[0] for row in cur.fetchall())

    rows = []
    skipped = 0
    for _, row in deliveries.iterrows():
        mid = int(row['match_id'])
        if mid not in valid_match_ids:
            skipped += 1
            continue
        rows.append((
            mid,
            int(row['inning']),
            players_map.get(row['batter']),
            players_map.get(row['bowler']),
            teams_map.get(row['batting_team']),
            int(row['batsman_runs']),
            int(row['extra_runs']),
            int(row['total_runs']),
            bool(row['is_wicket']),
            row['dismissal_kind'] if pd.notna(row.get('dismissal_kind')) else None
        ))

    execute_values(cur, """
        INSERT INTO fact_deliveries (
            match_id, inning, batsman_id, bowler_id, batting_team_id,
            runs_batsman, runs_extras, runs_total, is_wicket, dismissal_kind
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ fact_deliveries: {len(rows)} loaded, {skipped} skipped")

# ── Task 5: Validate loaded data ─────────────────────────────────
def validate_load(**kwargs):
    conn = get_conn()
    cur = conn.cursor()

    checks = {
        'dim_teams': 10,
        'dim_players': 100,
        'dim_venues': 10,
        'dim_matches': 500,
        'fact_deliveries': 100000,
    }

    all_passed = True
    for table, min_rows in checks.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        status = "✅" if count >= min_rows else "❌"
        print(f"{status} {table}: {count:,} rows (expected >= {min_rows:,})")
        if count < min_rows:
            all_passed = False

    cur.close()
    conn.close()

    if not all_passed:
        raise ValueError("Data validation failed — row counts below threshold")
    print("✅ All validation checks passed")

# ── Wire up the DAG ──────────────────────────────────────────────
start = EmptyOperator(task_id='start', dag=dag)
end   = EmptyOperator(task_id='end',   dag=dag)

t1 = PythonOperator(task_id='validate_source_files', python_callable=validate_source_files, dag=dag)
t2 = PythonOperator(task_id='extract_data',          python_callable=extract_data,          dag=dag)
t3 = PythonOperator(task_id='load_dimensions',       python_callable=load_dimensions,       dag=dag)
t4 = PythonOperator(task_id='load_facts',            python_callable=load_facts,            dag=dag)
t5 = PythonOperator(task_id='validate_load',         python_callable=validate_load,         dag=dag)

# ── DAG execution order ──────────────────────────────────────────
start >> t1 >> t2 >> t3 >> t4 >> t5 >> end