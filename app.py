import json
import psycopg2

# Database connection parameters
conn = psycopg2.connect(
    dbname="your_database_name", 
    user="your_username", 
    password="your_password", 
    host="localhost", 
    port="5432"  # or another port if you changed the default
)
cur = conn.cursor()

# Function to load JSON data
def load_data(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
        for item in data:
            # Example of extracting data: assume each item is an event
            player_id = item['player_id']
            team_id = item['team_id']
            game_id = item['game_id']
            event_type = item['type']
            timestamp = item['timestamp']
            outcome = item['outcome']
            
            # Insert data into the database
            cur.execute(
                "INSERT INTO Event (GameID, PlayerID, Type, Timestamp, Outcome) VALUES (%s, %s, %s, %s, %s)",
                (game_id, player_id, event_type, timestamp, outcome)
            )
            conn.commit()

# Specify your JSON file path
json_file_path = 'path_to_your_json_file.json'
load_data(json_file_path)

# Close the database connection
cur.close()
conn.close()
