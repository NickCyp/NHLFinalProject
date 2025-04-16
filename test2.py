import requests
import sqlite3
import time
import os
from requests.exceptions import RequestException

# --- CONFIGURATION ---
DB_NAME = "nhl_team_data.db"
TEAM_ABBREVIATION = "PIT"  # e.g., TOR, EDM, NYR
SEASON = "20242025"  # format: "YYYYYYYY"
TIMEOUT = 10  # Timeout for requests in seconds

# --- ENDPOINTS ---
SCHEDULE_URL = f"https://api-web.nhle.com/v1/club-schedule-season/{TEAM_ABBREVIATION}/{SEASON}"
PBP_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
PLAYERS_STATS_URL = "https://api.nhle.com/stats/rest/en/skater/summary?limit=-1&sort=points&cayenneExp=seasonId={season}"


def initialize_database():
    """Initialize SQLite database with required tables"""
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create games table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS games (
        game_id TEXT PRIMARY KEY,
        game_date TEXT,
        home_team TEXT,
        away_team TEXT,
        home_score INTEGER,
        away_score INTEGER,
        season TEXT
    )
    ''')

    # Create players table with only specified fields
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS players (
        player_id INTEGER PRIMARY KEY,
        full_name TEXT,
        team TEXT,
        position TEXT,
        games_played INTEGER,
        goals INTEGER,
        assists INTEGER,
        points INTEGER,
        plus_minus INTEGER,
        penalty_minutes INTEGER
    )
    ''')

    # Create hits table with coordinates
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hits (
        hit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id TEXT,
        period INTEGER,
        period_type TEXT,
        time_elapsed TEXT,
        xCoord INTEGER,
        yCoord INTEGER,
        hitter_id INTEGER,
        hittee_id INTEGER,
        FOREIGN KEY (game_id) REFERENCES games (game_id),
        FOREIGN KEY (hitter_id) REFERENCES players (player_id),
        FOREIGN KEY (hittee_id) REFERENCES players (player_id)
    )
    ''')

    conn.commit()
    conn.close()


def fetch_player_stats():
    """Fetch player stats from NHL Stats API"""
    try:
        url = PLAYERS_STATS_URL.format(season=SEASON)
        response = requests.get(url, timeout=TIMEOUT)
        if response.status_code == 200:
            return response.json().get('data', [])
        print(f"Failed to fetch player stats: {response.status_code}")
        return []
    except RequestException as e:
        print(f"Error fetching player stats: {e}")
        return []


def update_player_stats(conn):
    """Update player stats in the database using .get() for safe access"""
    cursor = conn.cursor()
    player_stats = fetch_player_stats()

    if not player_stats:
        print("No player stats data available.")
        return

    for player in player_stats:
        try:
            cursor.execute('''
            INSERT OR REPLACE INTO players (
                player_id, full_name, team, position,
                games_played, goals, assists, points, 
                plus_minus, penalty_minutes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                player.get('playerId'),
                player.get('skaterFullName', 'Unknown'),
                player.get('teamAbbrevs', 'UNK'),
                player.get('positionCode', 'U'),
                player.get('gamesPlayed', 0),
                player.get('goals', 0),
                player.get('assists', 0),
                player.get('points', 0),
                player.get('plusMinus', 0),
                player.get('penaltyMinutes', 0)
            ))
        except Exception as e:
            print(f"Error processing player data: {e}")
            continue

    conn.commit()
    print(f"Updated stats for {len(player_stats)} players")


def fetch_schedule():
    """Fetch the team's entire season schedule"""
    try:
        response = requests.get(SCHEDULE_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            return response.json().get('games', [])
        print(f"Failed to fetch schedule: {response.status_code}")
        return []
    except RequestException as e:
        print(f"Error fetching schedule: {e}")
        return []


def process_game(game_data, conn):
    """Process a single game's data including hit coordinates"""
    cursor = conn.cursor()

    # Insert game record with .get() for safe access
    cursor.execute('''
    INSERT OR IGNORE INTO games (game_id, game_date, home_team, away_team, home_score, away_score, season)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        game_data.get('id'),
        game_data.get('gameDate'),
        game_data.get('homeTeam', {}).get('abbrev', 'UNK'),
        game_data.get('awayTeam', {}).get('abbrev', 'UNK'),
        game_data.get('homeTeam', {}).get('score', 0),
        game_data.get('awayTeam', {}).get('score', 0),
        SEASON
    ))

    # Process play-by-play for hits
    pbp_url = PBP_URL.format(game_id=game_data.get('id'))
    try:
        pbp_response = requests.get(pbp_url, timeout=TIMEOUT)
        if pbp_response.status_code == 200:
            pbp = pbp_response.json()
            if pbp.get('plays'):
                for play in pbp['plays']:
                    if play.get('typeDescKey') == 'hit':
                        details = play.get('details', {})
                        hitter_id = details.get('hittingPlayerId')
                        hittee_id = details.get('hitteePlayerId')
                        period_info = play.get('periodDescriptor', {})
                        period_number = period_info.get('number', 0)
                        period_type = period_info.get('periodType', 'REG')

                        # Extract coordinates - check both play and details
                        x_coord = play.get('xCoord') or details.get('xCoord')
                        y_coord = play.get('yCoord') or details.get('yCoord')

                        # Only insert if we have both player IDs
                        if hitter_id and hittee_id:
                            try:
                                cursor.execute('''
                                INSERT INTO hits (
                                    game_id, period, period_type, time_elapsed,
                                    xCoord, yCoord, hitter_id, hittee_id
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    game_data.get('id'),
                                    period_number,
                                    period_type,
                                    play.get('timeInPeriod', '0:00'),
                                    x_coord,
                                    y_coord,
                                    hitter_id,
                                    hittee_id
                                ))
                            except sqlite3.Error as e:
                                print(f"Database error inserting hit: {e}")
                        else:
                            print(
                                f"Skipping hit record - missing player IDs: hitter_id={hitter_id}, hittee_id={hittee_id}")
    except RequestException as e:
        print(f"Error fetching play-by-play: {e}")

    conn.commit()


def main():
    print("Initializing database...")
    initialize_database()

    conn = sqlite3.connect(DB_NAME)

    print("Updating player stats...")
    update_player_stats(conn)

    print(f"Fetching schedule for {TEAM_ABBREVIATION}...")
    schedule = fetch_schedule()

    if not schedule:
        print("No schedule data available.")
        conn.close()
        return

    total_games = len([g for g in schedule if g.get('gameType') == 2])
    processed_games = 0

    print(f"Processing {total_games} regular season games...")
    for game in schedule:
        if game.get('gameType') == 2:  # Regular season only
            print(f"Processing game {game.get('id')} ({processed_games + 1}/{total_games})")
            try:
                process_game(game, conn)
                processed_games += 1
                time.sleep(1)  # Rate limiting
            except Exception as e:
                print(f"Error processing game: {e}")
                continue

    conn.close()
    print(f"Completed processing {processed_games} games.")


if __name__ == "__main__":
    main()