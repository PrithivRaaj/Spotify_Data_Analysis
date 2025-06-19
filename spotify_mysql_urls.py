

import re
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import mysql.connector
import pandas as pd

# Set up Spotify API credentials
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id='7110213d62a9472599feac5166ca751e',
    client_secret='8cb06209fef64ffaa61b8a913308cd59'
))

# MySQL Database Connection
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'spotifydata'
}

# Connect to the database
connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# Read track URLs from file
file_path = "track_urls.txt"
with open(file_path, 'r') as file:
    track_urls = file.readlines()

# 🟡 Collect all track data to save as CSV later
all_tracks_data = []

# Process each URL
for track_url in track_urls:
    track_url = track_url.strip()
    try:
        # Extract track ID from URL
        track_id = re.search(r'track/([a-zA-Z0-9]+)', track_url).group(1)

        # Fetch track details from Spotify API
        track = sp.track(track_id)

        # Extract metadata
        track_data = {
            'Track Name': track['name'],
            'Artist': track['artists'][0]['name'],
            'Album': track['album']['name'],
            'Popularity': track['popularity'],
            'Duration (minutes)': round(track['duration_ms'] / 60000, 2),
            'Explicit': track['explicit'],
            'Release Date': track['album']['release_date'],
            'Markets Count': len(track['available_markets']),

        }

        # ✅ Add to list for CSV
        all_tracks_data.append(track_data)
        
        check_query = """
        SELECT COUNT(*) FROM spotify_tracks WHERE track_name = %s AND artist = %s
        """
        cursor.execute(check_query, (track_data['Track Name'], track_data['Artist']))
        if cursor.fetchone()[0] == 0:

            # ✅ Insert into MySQL
            insert_query = """
            INSERT INTO spotify_tracks (track_name, artist, album, popularity, duration_minutes, explicit, release_date, markets_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                track_data['Track Name'],
                track_data['Artist'],
                track_data['Album'],
                track_data['Popularity'],
                track_data['Duration (minutes)'],
                track_data['Explicit'],
                track_data['Release Date'], 
                track_data['Markets Count']
            
                ))
            connection.commit()

            print(f"Inserted: {track_data['Track Name']} by {track_data['Artist']}")

    except Exception as e:
        print(f"❌ Error processing URL: {track_url}, Error: {e}")

# ✅ Save all data to CSV after loop
if all_tracks_data:
    df = pd.DataFrame(all_tracks_data)
    df.to_csv('spotify_track_data.csv', index=False)
    print("✅ CSV file 'spotify_track_data.csv' saved successfully.")
else:
    print("⚠️ No track data to save to CSV.")

# ✅ Close DB connection
cursor.close()
connection.close()
print("✅ All tracks have been processed and inserted into the database.")
