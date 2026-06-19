-- ── IPL Analytics Pipeline — Star Schema DDL ──────────────────
-- Run this script to initialise the database from scratch
-- Author: Dinesh Reddy

-- Dimension Tables
CREATE TABLE IF NOT EXISTS dim_teams (
    team_id   SERIAL PRIMARY KEY,
    team_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_players (
    player_id   SERIAL PRIMARY KEY,
    player_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_venues (
    venue_id   SERIAL PRIMARY KEY,
    venue_name VARCHAR(200) UNIQUE NOT NULL,
    city       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_matches (
    match_id            INT PRIMARY KEY,
    season              INT,
    match_date          DATE,
    venue_id            INT REFERENCES dim_venues(venue_id),
    team1_id            INT REFERENCES dim_teams(team_id),
    team2_id            INT REFERENCES dim_teams(team_id),
    toss_winner_id      INT REFERENCES dim_teams(team_id),
    toss_decision       VARCHAR(10),
    winner_id           INT REFERENCES dim_teams(team_id),
    player_of_match_id  INT REFERENCES dim_players(player_id),
    result_margin       FLOAT
);

-- Fact Table
CREATE TABLE IF NOT EXISTS fact_deliveries (
    delivery_id     SERIAL PRIMARY KEY,
    match_id        INT REFERENCES dim_matches(match_id),
    inning          INT,
    batsman_id      INT REFERENCES dim_players(player_id),
    bowler_id       INT REFERENCES dim_players(player_id),
    batting_team_id INT REFERENCES dim_teams(team_id),
    runs_batsman    INT,
    runs_extras     INT,
    runs_total      INT,
    is_wicket       BOOLEAN,
    dismissal_kind  VARCHAR(50)
);