import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ── Database connection ──────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ipl_db",
        user="ipl_user",
        password="ipl_pass"
    )

# ── Load dim_teams ───────────────────────────────────────────────
def load_teams(conn, matches, deliveries):
    teams = set(matches['team1'].dropna()) | set(matches['team2'].dropna())
    cur = conn.cursor()
    for team in sorted(teams):
        cur.execute("""
            INSERT INTO dim_teams (team_name)
            VALUES (%s)
            ON CONFLICT (team_name) DO NOTHING
        """, (team,))
    conn.commit()
    cur.close()
    print(f"✅ dim_teams loaded: {len(teams)} teams")

# ── Load dim_players ─────────────────────────────────────────────
def load_players(conn, matches, deliveries):
    players = (
        set(deliveries['batter'].dropna()) |
        set(deliveries['bowler'].dropna()) |
        set(matches['player_of_match'].dropna())
    )
    cur = conn.cursor()
    for player in sorted(players):
        cur.execute("""
            INSERT INTO dim_players (player_name)
            VALUES (%s)
            ON CONFLICT (player_name) DO NOTHING
        """, (player,))
    conn.commit()
    cur.close()
    print(f"✅ dim_players loaded: {len(players)} players")

# ── Load dim_venues ──────────────────────────────────────────────
def load_venues(conn, matches):
    venues = matches[['venue', 'city']].drop_duplicates().dropna(subset=['venue'])
    cur = conn.cursor()
    for _, row in venues.iterrows():
        cur.execute("""
            INSERT INTO dim_venues (venue_name, city)
            VALUES (%s, %s)
            ON CONFLICT (venue_name) DO NOTHING
        """, (row['venue'], row.get('city')))
    conn.commit()
    cur.close()
    print(f"✅ dim_venues loaded: {len(venues)} venues")

# ── Load dim_matches ─────────────────────────────────────────────
def load_matches(conn, matches):
    cur = conn.cursor()

    cur.execute("SELECT team_name, team_id FROM dim_teams")
    teams = dict(cur.fetchall())

    cur.execute("SELECT player_name, player_id FROM dim_players")
    players = dict(cur.fetchall())

    cur.execute("SELECT venue_name, venue_id FROM dim_venues")
    venues = dict(cur.fetchall())

    # Clean season — handle formats like '2007/08', '2009/10', '2020/21'
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
                venues.get(row['venue']),
                teams.get(row['team1']),
                teams.get(row['team2']),
                teams.get(row['toss_winner']),
                row['toss_decision'] if pd.notna(row['toss_decision']) else None,
                teams.get(row['winner']) if pd.notna(row.get('winner')) else None,
                players.get(row['player_of_match']) if pd.notna(row.get('player_of_match')) else None,
                float(row['result_margin']) if pd.notna(row.get('result_margin')) else None
            ))
            loaded += 1
        except Exception as e:
            print(f"  ⚠️ Skipped match {row['id']}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    print(f"✅ dim_matches loaded: {loaded} matches")

# ── Load fact_deliveries ─────────────────────────────────────────
def load_deliveries(conn, deliveries):
    cur = conn.cursor()

    cur.execute("SELECT team_name, team_id FROM dim_teams")
    teams = dict(cur.fetchall())

    cur.execute("SELECT player_name, player_id FROM dim_players")
    players = dict(cur.fetchall())

    # Only load deliveries for matches that exist in dim_matches
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
            players.get(row['batter']),
            players.get(row['bowler']),
            teams.get(row['batting_team']),
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
    """, rows)

    conn.commit()
    cur.close()
    print(f"✅ fact_deliveries loaded: {len(rows)} deliveries ({skipped} skipped)")

# ── Main ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("📂 Reading CSV files...")
    matches = pd.read_csv("data/matches.csv")
    deliveries = pd.read_csv("data/deliveries.csv")

    print("🔌 Connecting to database...")
    conn = get_conn()

    print("\n⏳ Loading dimension tables...")
    load_teams(conn, matches, deliveries)
    load_players(conn, matches, deliveries)
    load_venues(conn, matches)
    load_matches(conn, matches)

    print("\n⏳ Loading fact table (260k rows, takes ~30 seconds)...")
    load_deliveries(conn, deliveries)

    conn.close()
    print("\n🎉 All data loaded successfully!")