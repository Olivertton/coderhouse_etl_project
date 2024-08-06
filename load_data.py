import psycopg2
import pandas as pd

# Configuración de la conexión
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'
dbname = 'data-engineer-database'
user = 'irving_ramirez_coderhouse'
password = 'T93hbU4sqc'

# Leer el archivo CSV
df = pd.read_csv('movies_2024.csv')

# Conectar a Redshift
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cur = conn.cursor()

# Función para convertir los valores de Ratings
def convert_ratings(ratings):
    if pd.isna(ratings):
        return None, None, None
    else:
        ratings_list = eval(ratings)
        imdb_rating = next((r['Value'] for r in ratings_list if r['Source'] == 'Internet Movie Database'), None)
        rotten_tomatoes_rating = next((r['Value'] for r in ratings_list if r['Source'] == 'Rotten Tomatoes'), None)
        metacritic_rating = next((r['Value'] for r in ratings_list if r['Source'] == 'Metacritic'), None)
        return imdb_rating, rotten_tomatoes_rating, metacritic_rating

# Función para truncar valores
def truncate_value(value, max_length):
    if pd.isna(value):
        return None
    return str(value)[:max_length]

# Insertar los datos en la tabla
insert_query = """
INSERT INTO movies_2024 (Title, Year, Rated, Released, Runtime, Genre, Director, Writer, Actors, Plot, Language, Country, Awards, Poster, Metascore, imdbRating, imdbVotes, imdbID, Type, DVD, BoxOffice, Production, Website, Response, IMDB_Rating, Rotten_Tomatoes_Rating, Metacritic_Rating)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for index, row in df.iterrows():
    imdb_rating, rotten_tomatoes_rating, metacritic_rating = convert_ratings(row['Ratings'])
    cur.execute(insert_query, (
        truncate_value(row['Title'], 256), row['Year'], truncate_value(row['Rated'], 10), truncate_value(row['Released'], 50),
        truncate_value(row['Runtime'], 50), truncate_value(row['Genre'], 256), truncate_value(row['Director'], 256),
        truncate_value(row['Writer'], 256), truncate_value(row['Actors'], 512), truncate_value(row['Plot'], 1000),
        truncate_value(row['Language'], 100), truncate_value(row['Country'], 100), truncate_value(row['Awards'], 512),
        truncate_value(row['Poster'], 512), truncate_value(row['Metascore'], 10), truncate_value(row['imdbRating'], 10),
        truncate_value(row['imdbVotes'], 50), truncate_value(row['imdbID'], 50), truncate_value(row['Type'], 50),
        truncate_value(row['DVD'], 50), truncate_value(row['BoxOffice'], 100), truncate_value(row['Production'], 256),
        truncate_value(row['Website'], 256), str(row['Response']), imdb_rating, rotten_tomatoes_rating, metacritic_rating
    ))
    conn.commit()

# Cerrar la conexión
cur.close()
conn.close()
print("Datos cargados exitosamente en Redshift")