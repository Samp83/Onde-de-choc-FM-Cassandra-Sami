import os
import json
import pymysql
import time

# Configuration de la connexion MySQL
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = ""
DB_NAME = "spotify_data"

# Démarrer le compteur de temps
start_time = time.time()

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
        track_uri VARCHAR(191) PRIMARY KEY,
        artist_uri VARCHAR(191),
        track_name VARCHAR(255),
        album_uri VARCHAR(191),
        duration_ms INT,
        FOREIGN KEY (artist_uri) REFERENCES artists(artist_uri) ON DELETE CASCADE,
        FOREIGN KEY (album_uri) REFERENCES albums(album_uri) ON DELETE CASCADE
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS playlist_tracks (
        pid INT,
        track_uri VARCHAR(191),
        pos INT,
        PRIMARY KEY (pid, track_uri),  -- Clé composite
        FOREIGN KEY (pid) REFERENCES playlists(pid) ON DELETE CASCADE,
        FOREIGN KEY (track_uri) REFERENCES tracks(track_uri) ON DELETE CASCADE
    )
""")

# Parcourir le dossier "data" et traiter les fichiers JSON
DATA_FOLDER = "./data"

# Listes pour batch insert
playlists_data = []
artists_data = set()  # Utilisation d'un set pour éviter les doublons
albums_data = set()
tracks_data = []
playlist_tracks_data = []

for file_name in os.listdir(DATA_FOLDER):
    if file_name.endswith(".json"):
        file_path = os.path.join(DATA_FOLDER, file_name)
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            
            # Insérer chaque playlist
            for playlist in data["playlists"]:
                playlists_data.append((
                    playlist["pid"], playlist["name"], playlist["collaborative"] == "true",
                    playlist["modified_at"], playlist["num_tracks"], playlist["num_albums"],
                    playlist["num_followers"], playlist["num_edits"], playlist["duration_ms"], playlist["num_artists"]
                ))

                # Insérer les morceaux et leurs artistes/albums
                for track in playlist["tracks"]:
                    artists_data.add((track["artist_uri"], track["artist_name"]))
                    albums_data.add((track["album_uri"], track["album_name"]))
                    tracks_data.append((
                        track["track_uri"], track["artist_uri"], track["track_name"],
                        track["album_uri"], track["duration_ms"]
                    ))
                    playlist_tracks_data.append((playlist["pid"], track["track_uri"], track["pos"]))

# Insertion des playlists en batch
if playlists_data:
    cursor.executemany("""
        INSERT INTO playlists (pid, name, collaborative, modified_at, num_tracks, num_albums, num_followers, num_edits, duration_ms, num_artists)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        name=VALUES(name), collaborative=VALUES(collaborative), modified_at=VALUES(modified_at),
        num_tracks=VALUES(num_tracks), num_albums=VALUES(num_albums), num_followers=VALUES(num_followers),
        num_edits=VALUES(num_edits), duration_ms=VALUES(duration_ms), num_artists=VALUES(num_artists)
    """, playlists_data)

# Insertion des artistes en batch (SET évite les doublons)
if artists_data:
    cursor.executemany("""
        INSERT IGNORE INTO artists (artist_uri, artist_name) VALUES (%s, %s)
    """, list(artists_data))

# Insertion des albums en batch
if albums_data:
    cursor.executemany("""
        INSERT IGNORE INTO albums (album_uri, album_name) VALUES (%s, %s)
    """, list(albums_data))

# Insertion des tracks en batch
if tracks_data:
    cursor.executemany("""
        INSERT INTO tracks (track_uri, artist_uri, track_name, album_uri, duration_ms)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        track_name=VALUES(track_name), duration_ms=VALUES(duration_ms)
    """, tracks_data)

# Insertion des relations Playlist ↔ Tracks en batch avec clé composite
if playlist_tracks_data:
    cursor.executemany("""
        INSERT INTO playlist_tracks (pid, track_uri, pos)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE pos=VALUES(pos)
    """, playlist_tracks_data)

# Valider les changements et fermer la connexion
conn.commit()
cursor.close()
conn.close()

# Calculer et afficher le temps d'exécution
end_time = time.time()
execution_time = end_time - start_time
print(f"Données insérées avec succès en {execution_time:.2f} secondes !")
