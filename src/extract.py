import os
import orjson
import pymysql
import time
from concurrent.futures import ThreadPoolExecutor

# Configuration MariaDB
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = ""
DB_NAME = "spotify_data"
DATA_FOLDER = "./data"

# Démarrer le compteur de temps
start_time = time.time()

# Connexion initiale pour créer la base de données
conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD)
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
cursor.close()
conn.close()

# Connexion à la base créée
conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=False)
cursor = conn.cursor()

# Désactivation temporaire des clés étrangères et des index


# Création des tables
cursor.execute("""
    CREATE TABLE IF NOT EXISTS playlists (
        pid INT PRIMARY KEY,
        name VARCHAR(255),
        collaborative TINYINT(1),  
        modified_at INT,
        num_tracks INT,
        num_albums INT,
        num_followers INT,
        num_edits INT,
        duration_ms INT,
        num_artists INT
    ) ENGINE=InnoDB;
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_uri VARCHAR(191) PRIMARY KEY,
        artist_name VARCHAR(255)
    ) ENGINE=InnoDB;
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS albums (
        album_uri VARCHAR(191) PRIMARY KEY,
        album_name VARCHAR(255)
    ) ENGINE=InnoDB;
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
    ) ENGINE=InnoDB;
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS playlist_tracks (
        pid INT,
        track_uri VARCHAR(191),
        pos INT,
        PRIMARY KEY (pid, track_uri),  
        FOREIGN KEY (pid) REFERENCES playlists(pid) ON DELETE CASCADE,
        FOREIGN KEY (track_uri) REFERENCES tracks(track_uri) ON DELETE CASCADE
    ) ENGINE=InnoDB;
""")


cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
cursor.execute("ALTER TABLE playlists DISABLE KEYS;")
cursor.execute("ALTER TABLE artists DISABLE KEYS;")
cursor.execute("ALTER TABLE albums DISABLE KEYS;")
cursor.execute("ALTER TABLE tracks DISABLE KEYS;")
cursor.execute("ALTER TABLE playlist_tracks DISABLE KEYS;")

conn.commit()

# **Chargement des fichiers JSON en parallèle**
def process_file(file_path):
    local_playlists = []
    local_artists = set()
    local_albums = set()
    local_tracks = []
    local_playlist_tracks = []

    with open(file_path, "rb") as file:
        data = orjson.loads(file.read())

    for playlist in data["playlists"]:
        local_playlists.append((
            playlist["pid"], playlist["name"], playlist["collaborative"] == "true",
            playlist["modified_at"], playlist["num_tracks"], playlist["num_albums"],
            playlist["num_followers"], playlist["num_edits"], playlist["duration_ms"], playlist["num_artists"]
        ))

        for track in playlist["tracks"]:
            local_artists.add((track["artist_uri"], track["artist_name"]))
            local_albums.add((track["album_uri"], track["album_name"]))
            local_tracks.append((
                track["track_uri"], track["artist_uri"], track["track_name"],
                track["album_uri"], track["duration_ms"]
            ))
            local_playlist_tracks.append((playlist["pid"], track["track_uri"], track["pos"]))

    return local_playlists, local_artists, local_albums, local_tracks, local_playlist_tracks

# Traitement multi-threading pour lire les fichiers
with ThreadPoolExecutor() as executor:
    file_paths = [os.path.join(DATA_FOLDER, f) for f in os.listdir(DATA_FOLDER) if f.endswith(".json")]
    results = executor.map(process_file, file_paths)

# Fusion de toutes les données
playlists_data, artists_data, albums_data, tracks_data, playlist_tracks_data = [], set(), set(), [], []
for result in results:
    playlists_data.extend(result[0])
    artists_data.update(result[1])
    albums_data.update(result[2])
    tracks_data.extend(result[3])
    playlist_tracks_data.extend(result[4])

# **Insertion unique pour toutes les données**
query = """
    INSERT INTO playlists (pid, name, collaborative, modified_at, num_tracks, num_albums, num_followers, num_edits, duration_ms, num_artists)
    VALUES %s
    ON DUPLICATE KEY UPDATE 
    name=VALUES(name), collaborative=VALUES(collaborative), modified_at=VALUES(modified_at),
    num_tracks=VALUES(num_tracks), num_albums=VALUES(num_albums), num_followers=VALUES(num_followers),
    num_edits=VALUES(num_edits), duration_ms=VALUES(duration_ms), num_artists=VALUES(num_artists);
""" % ",".join(str(tuple(x)) for x in playlists_data)
cursor.execute(query)

cursor.execute("""
    INSERT IGNORE INTO artists (artist_uri, artist_name) VALUES 
    %s;
""" % ",".join(str(tuple(x)) for x in artists_data))

cursor.execute("""
    INSERT IGNORE INTO albums (album_uri, album_name) VALUES 
    %s;
""" % ",".join(str(tuple(x)) for x in albums_data))

query = """
    INSERT INTO tracks (track_uri, artist_uri, track_name, album_uri, duration_ms)
    VALUES %s
    ON DUPLICATE KEY UPDATE track_name=VALUES(track_name), duration_ms=VALUES(duration_ms);
""" % ",".join(str(tuple(x)) for x in tracks_data)
cursor.execute(query)

cursor.execute("""
    INSERT IGNORE INTO playlist_tracks (pid, track_uri, pos) VALUES 
    %s;
""" % ",".join(str(tuple(x)) for x in playlist_tracks_data))

# **Réactivation des index et clés**
cursor.execute("ALTER TABLE playlists ENABLE KEYS;")
cursor.execute("ALTER TABLE artists ENABLE KEYS;")
cursor.execute("ALTER TABLE albums ENABLE KEYS;")
cursor.execute("ALTER TABLE tracks ENABLE KEYS;")
cursor.execute("ALTER TABLE playlist_tracks ENABLE KEYS;")
cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

# Commit final et fermeture
conn.commit()
cursor.close()
conn.close()

# Temps d'exécution
execution_time = time.time() - start_time
print(f"Données insérées avec succès en {execution_time:.3f} secondes !")
