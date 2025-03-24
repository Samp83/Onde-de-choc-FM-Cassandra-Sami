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

# Connexion à la base de données
conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=False)
cursor = conn.cursor()

# Désactivation temporaire des contraintes pour optimisation
cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
cursor.execute("ALTER TABLE playlists DISABLE KEYS;")
cursor.execute("ALTER TABLE artists DISABLE KEYS;")
cursor.execute("ALTER TABLE albums DISABLE KEYS;")
cursor.execute("ALTER TABLE tracks DISABLE KEYS;")
cursor.execute("ALTER TABLE playlist_tracks DISABLE KEYS;")
conn.commit()

# **Chargement des fichiers JSON en parallèle**
def process_file(file_path):
    playlists = []
    artists = set()
    albums = set()
    tracks = set()
    playlist_tracks = []

    with open(file_path, "rb") as file:
        data = orjson.loads(file.read())

    for playlist in data["playlists"]:
        playlists.append((
            playlist["pid"], playlist["name"], playlist["collaborative"] == "true",
            playlist["modified_at"], playlist["num_tracks"], playlist["num_albums"],
            playlist["num_followers"], playlist["num_edits"], playlist["duration_ms"], playlist["num_artists"]
        ))

        for track in playlist["tracks"]:
            artists.add((track["artist_uri"], track["artist_name"]))
            albums.add((track["album_uri"], track["album_name"]))
            tracks.add((
                track["track_uri"], track["artist_uri"], track["track_name"],
                track["album_uri"], track["duration_ms"]
            ))
            playlist_tracks.append((playlist["pid"], track["track_uri"], track["pos"]))

    return playlists, artists, albums, tracks, playlist_tracks

# Traitement multi-threading pour lire les fichiers
with ThreadPoolExecutor() as executor:
    file_paths = [os.path.join(DATA_FOLDER, f) for f in os.listdir(DATA_FOLDER) if f.endswith(".json")]
    results = executor.map(process_file, file_paths)

# Fusion des données en RAM (évite les doublons avant insertion)
playlists_data, artists_data, albums_data, tracks_data, playlist_tracks_data = [], set(), set(), set(), []
for result in results:
    playlists_data.extend(result[0])
    artists_data.update(result[1])
    albums_data.update(result[2])
    tracks_data.update(result[3])
    playlist_tracks_data.extend(result[4])

# **Insertion optimisée par batch**
BATCH_SIZE = 5000  # Ajustable selon les ressources

def batch_insert(query, data):
    """ Insère les données par lot pour éviter surcharge mémoire/disque """
    if data:
        cursor.executemany(query, list(data))

# **1. Insertion des playlists**
batch_insert("""
    INSERT INTO playlists (pid, name, collaborative, modified_at, num_tracks, num_albums, num_followers, num_edits, duration_ms, num_artists)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    name=VALUES(name), collaborative=VALUES(collaborative), modified_at=VALUES(modified_at),
    num_tracks=VALUES(num_tracks), num_albums=VALUES(num_albums), num_followers=VALUES(num_followers),
    num_edits=VALUES(num_edits), duration_ms=VALUES(duration_ms), num_artists=VALUES(num_artists);
""", playlists_data)

# **2. Insertion des artistes (uniques)**
batch_insert("""
    INSERT IGNORE INTO artists (artist_uri, artist_name) VALUES (%s, %s);
""", artists_data)

# **3. Insertion des albums (uniques)**
batch_insert("""
    INSERT IGNORE INTO albums (album_uri, album_name) VALUES (%s, %s);
""", albums_data)

# **4. Insertion des tracks**
batch_insert("""
    INSERT INTO tracks (track_uri, artist_uri, track_name, album_uri, duration_ms)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE track_name=VALUES(track_name), duration_ms=VALUES(duration_ms);
""", tracks_data)

# **5. Insertion des relations playlist_tracks**
batch_insert("""
    INSERT IGNORE INTO playlist_tracks (pid, track_uri, pos) VALUES (%s, %s, %s);
""", playlist_tracks_data)

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
