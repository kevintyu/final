import json
import psycopg2
import os

# Database connection parameters
conn = psycopg2.connect(
    dbname="final", 
    user="postgres", 
    password="123456", 
    host="localhost", 
    port="5432"  # or another port if you changed the default
)

def import_seasons(seasons_file_path, cursor):
    # Open the JSON file containing competitions data
    with open(seasons_file_path, 'r') as file:
        # Load the JSON data from the file
        seasons = json.load(file)
        
        # Iterate through each competition in the JSON data
        for season in seasons:
            # Extract the competition ID and name from each JSON object
            season_id = season.get('season_id')
            season_name = season.get('season_name')
            # Construct the SQL INSERT statement
            insert_query = """
                INSERT INTO Season (SeasonID, SeasonName)
                VALUES (%s, %s)
                ON CONFLICT (SeasonID) DO NOTHING;
            """
            
            # Execute the SQL INSERT statement with the competition ID and name
            cursor.execute(insert_query, (season_id, season_name))
            # Commit the transaction
        conn.commit()

def import_competitions(competitions_file_path, cursor):
    # Open the JSON file containing competitions data
    with open(competitions_file_path, 'r') as file:
        # Load the JSON data from the file
        competitions = json.load(file)
        
        # Iterate through each competition in the JSON data
        for competition in competitions:
            # Extract the competition ID and name from each JSON object
            competition_id = competition.get('competition_id')
            competition_name = competition.get('competition_name')
            season_id = competition.get('season_id')
            # Construct the SQL INSERT statement
            insert_query = """
                INSERT INTO Competition (CompetitionID, CompetitionName, SeasonID)
                VALUES (%s, %s, %s)
                ON CONFLICT (CompetitionID) DO NOTHING;
            """
            
            # Execute the SQL INSERT statement with the competition ID and name
            cursor.execute(insert_query, (competition_id, competition_name, season_id))
            # Commit the transaction
        conn.commit()


def import_matches(matches_directory, cursor):
    # Iterate over every file within the matches directory and its subdirectories
    for root, dirs, files in os.walk(matches_directory):
        for file in files:
            # Skip non-JSON files
            if not file.endswith('.json'):
                continue

            file_path = os.path.join(root, file)
            with open(file_path, 'r', encoding='utf-8') as json_file:
                matches = json.load(json_file)
                for match in matches:
                    # Extract the necessary information from each match object
                    game_id = match.get('match_id')
                    competition_id = match['competition'].get('competition_id')
                    season_id = match['season'].get('season_id')

                    # Debug output
                    # print(f"Inserting match with GameID: {game_id}, CompetitionID: {competition_id}, SeasonID: {season_id}")

                    # Prepare the SQL INSERT statement
                    insert_query = """
                        INSERT INTO Game (GameID, SeasonID, CompetitionID)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (GameID) DO NOTHING;
                    """
                    # Execute the insert statement
                    try:
                        cursor.execute(insert_query, (game_id, season_id, competition_id))
                    except psycopg2.IntegrityError as e:
                        print(f"Integrity error: {e}")
                        cursor.connection.rollback()
                    except Exception as e:
                        print(f"Unexpected error: {e}")
                        cursor.connection.rollback()
                    else:
                        cursor.connection.commit()

def import_teams(teams_directory, cursor):
    # Iterate over all the JSON files in the directory
    for filename in os.listdir(teams_directory):
        if filename.endswith('.json'):
            file_path = os.path.join(teams_directory, filename)

            # Open and load the JSON file
            with open(file_path, 'r', encoding='utf-8') as json_file:
                teams = json.load(json_file)  # A list of teams

                for team in teams:
                    team_id = team['team_id']
                    team_name = team['team_name']

                    # Prepare the SQL INSERT statement
                    insert_query = """
                        INSERT INTO Team (TeamID, TeamName)
                        VALUES (%s, %s)
                        ON CONFLICT (TeamID) DO NOTHING;
                    """
                    # Execute the insert statement
                    cursor.execute(insert_query, (team_id, team_name))
                
                # Commit after each file is processed to minimize transaction scope
                cursor.connection.commit()

def import_players(players_directory, cursor):
    # Iterate over all the JSON files in the directory
    for filename in os.listdir(players_directory):
        if filename.endswith('.json'):
            file_path = os.path.join(players_directory, filename)

            # Open and load the JSON file
            with open(file_path, 'r', encoding='utf-8') as json_file:
                data = json.load(json_file)  # A list containing team information

                for team in data:
                    team_id = team['team_id']
                    for player in team['lineup']:
                        player_id = player['player_id']
                        player_name = player['player_name']
                        
                        # Prepare the SQL INSERT statement
                        insert_query = """
                            INSERT INTO Player (playerID, name, teamid)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (playerID) DO NOTHING;
                        """
                        # Execute the insert statement
                        cursor.execute(insert_query, (player_id, player_name, team_id))

                # Commit after each file is processed to minimize transaction scope
                cursor.connection.commit()

def import_event_types(events_directory, cursor):
    valid_types = {'Dribble': 9, 'Shot': 16, 'Pass': 30}
    print("running")
    for key in valid_types:
            insert_query = """
                INSERT INTO EventType (TypeName, TypeID)
                VALUES (%s, %s)
                ON CONFLICT (TypeID) DO NOTHING;
            """
            cursor.execute(insert_query, (key, valid_types[key]))

    # Commit after each file is processed
    print("done")
    cursor.connection.commit()

def import_events(events_directory, cursor):
    # Define event types to include
    valid_types = {'Dribble': 14, 'Shot': 16, 'Pass': 30}
    # event_type_ids 
    
    # Iterate over all JSON files in the directory
    for filename in os.listdir(events_directory):
        if filename.endswith('.json'):
            gameid = int(filename.split('.')[0])  # Extract gameID from the filename
            file_path = os.path.join(events_directory, filename)
            
            with open(file_path, 'r', encoding='utf-8') as json_file:
                events = json.load(json_file)

                for event in events:
                    event_type_name = event['type']['name']
                    if event_type_name in valid_types:
                        # print(event_type_name)
                        eventid = event['id']
                        type_id = event['type']['id']
                        player_id = event['player']['id']
                        xg = event['shot']['statsbomb_xg'] if event_type_name == 'Shot' and 'shot' in event else None
                        first_time = event['shot']['first_time'] if event_type_name == 'Shot' and 'first_time' in event['shot'] else False
                        intended_recipient_id = event['pass']['recipient']['id'] if event_type_name == 'Pass' and 'recipient' in event['pass'] else None
                        complete = event['dribble']['outcome']['name'] == 'Complete' if event_type_name == 'Dribble' and 'outcome' in event['dribble'] else None

                        # Prepare SQL INSERT statement
                        insert_query = """
                            INSERT INTO event (eventID, gameID, playerID, XG, FirstTime, type, IntendedRecipient, Complete, TypeID)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (eventID) DO NOTHING;
                        """
                        cursor.execute(insert_query, (eventid, gameid, player_id, xg, first_time, event_type_name, intended_recipient_id, complete, type_id))

                # Commit after each file is processed
                cursor.connection.commit()

def import_played_in(teams_directory, cursor):
    # Iterate over all the JSON files in the directory
    for filename in os.listdir(teams_directory):
        if filename.endswith('.json'):
            gameid = int(filename.split('.')[0])  # Extract gameID from the filename
            file_path = os.path.join(teams_directory, filename)

            # Open and load the JSON file
            with open(file_path, 'r', encoding='utf-8') as json_file:
                teams = json.load(json_file)  # A list of teams

                for team in teams:
                    team_id = team['team_id']

                    # Prepare the SQL INSERT statement
                    insert_query = """
                        INSERT INTO playedIn (GameID, TeamID)
                        VALUES (%s, %s)
                    """
                    # Execute the insert statement
                    cursor.execute(insert_query, (gameid, team_id))
                
                # Commit after each file is processed to minimize transaction scope
                cursor.connection.commit()



cur = conn.cursor()



# Specify your JSON file path

import_seasons('open-data/data/competitions.json', cur)
import_competitions('open-data/data/competitions.json', cur)
import_matches('open-data/data/matches', cur)
import_teams('open-data/data/lineups', cur)
import_players('open-data/data/lineups', cur)
import_event_types('open-data/data/events', cur)
import_played_in('open-data/data/lineups', cur)
import_events('open-data/data/events', cur)
# Close the database connection
cur.close()
conn.close()