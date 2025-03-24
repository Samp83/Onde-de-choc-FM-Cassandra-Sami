import os
import json
import pymysql

# Configuration de la connexion MySQL
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = ""
DB_NAME = "spotify_data"

# Connexion à MySQL
conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD)
cursor = conn.cursor()

# Création de la base de données et sélection de la base
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
cursor.execute(f"USE {DB_NAME}")

# Création des tables
cursor.execute("""
    CREATE TABLE IF NOT EXISTS playlists (
        pid INT PRIMARY KEY,
        name VARCHAR(255),
        collaborative BOOLEAN,
        modified_at INT,
        num_tracks INT,
        num_albums INT,
        num_followers INT,
        num_edits INT,
        duration_ms INT,
        num_artists INT
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_uri VARCHAR(191) PRIMARY KEY,
        artist_name VARCHAR(255)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS albums (
        album_uri VARCHAR(191) PRIMARY KEY,
        album_name VARCHAR(255)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
        id INT AUTO_INCREMENT PRIMARY KEY,
        pid INT,
        artist_uri VARCHAR(255),
        track_uri VARCHAR(191) UNIQUE,
        track_name VARCHAR(255),
        album_uri VARCHAR(255),
        duration_ms INT,
        pos INT,
        FOREIGN KEY (pid) REFERENCES playlists(pid),
        FOREIGN KEY (artist_uri) REFERENCES artists(artist_uri),
        FOREIGN KEY (album_uri) REFERENCES albums(album_uri)
    )
""")

# Fonction pour insérer une playlist
def insert_playlist(playlist):
    cursor.execute("""
        INSERT INTO playlists (pid, name, collaborative, modified_at, num_tracks, num_albums, num_followers, num_edits, duration_ms, num_artists)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        name=VALUES(name), collaborative=VALUES(collaborative), modified_at=VALUES(modified_at),
        num_tracks=VALUES(num_tracks), num_albums=VALUES(num_albums), num_followers=VALUES(num_followers),
        num_edits=VALUES(num_edits), duration_ms=VALUES(duration_ms), num_artists=VALUES(num_artists)
    """, (
        playlist["pid"], playlist["name"], playlist["collaborative"] == "true",
        playlist["modified_at"], playlist["num_tracks"], playlist["num_albums"],
        playlist["num_followers"], playlist["num_edits"], playlist["duration_ms"], playlist["num_artists"]
    ))

# Fonction pour insérer un artiste
def insert_artist(artist_uri, artist_name):
    cursor.execute("""
        INSERT IGNORE INTO artists (artist_uri, artist_name) VALUES (%s, %s)
    """, (artist_uri, artist_name))

# Fonction pour insérer un album
def insert_album(album_uri, album_name):
    cursor.execute("""
        INSERT IGNORE INTO albums (album_uri, album_name) VALUES (%s, %s)
    """, (album_uri, album_name))

# Fonction pour insérer un track
def insert_track(track, pid):
    cursor.execute("""
        INSERT INTO tracks (pid, artist_uri, track_uri, track_name, album_uri, duration_ms, pos)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        track_name=VALUES(track_name), duration_ms=VALUES(duration_ms), pos=VALUES(pos)
    """, (
        pid, track["artist_uri"], track["track_uri"], track["track_name"],
        track["album_uri"], track["duration_ms"], track["pos"]
    ))

# Parcourir le dossier "data" et traiter les fichiers JSON
DATA_FOLDER = "./data"

for file_name in os.listdir(DATA_FOLDER):
    if file_name.endswith(".json"):
        file_path = os.path.join(DATA_FOLDER, file_name)
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            
            # Insérer chaque playlist
            for playlist in data["playlists"]:
                insert_playlist(playlist)

                # Insérer les morceaux et leurs artistes/albums
                for track in playlist["tracks"]:
                    insert_artist(track["artist_uri"], track["artist_name"])
                    insert_album(track["album_uri"], track["album_name"])
                    insert_track(track, playlist["pid"])

# Valider les changements et fermer la connexion
conn.commit()
cursor.close()
conn.close()

print("Données insérées avec succès !")
