import psycopg2
import pandas as pd
import os
from datetime import datetime

# Configuración de la conexión desde variables de entorno
host = os.getenv('REDSHIFT_HOST')
port = os.getenv('REDSHIFT_PORT')
dbname = os.getenv('REDSHIFT_DBNAME')
user = os.getenv('REDSHIFT_USER')
password = os.getenv('REDSHIFT_PASSWORD')

# Leer el archivo CSV
df = pd.read_csv('movies_2024.csv')

# Convertir columnas específicas a cadenas para evitar problemas de inserción
columns_to_convert = ['Metascore', 'imdbRating', 'Website', 'Response', 'IMDB_Rating', 'Rotten_Tomatoes_Rating', 'Metacritic_Rating']
for column in columns_to_convert:
    df[column] = df[column].fillna('').astype(str)

# Agregar columna ingestion_timestamp con la marca de tiempo actual
df['ingestion_timestamp'] = datetime.now()

# Conectar a Redshift
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cur = conn.cursor()

# Función para truncar valores si es necesario
def truncate_value(value, max_length):
    if pd.isna(value) or value == '':
        return None
    return str(value)[:max_length]

# Query de inserción de datos (incluyendo ingestion_timestamp)
insert_query = """
INSERT INTO movies_2024 (Title, Year, Rated, Released, Runtime, Genre, Director, Writer, Actors, Plot, Language, Country, Awards, Poster, Metascore, imdbRating, imdbVotes, imdbID, Type, DVD, BoxOffice, Production, Website, Response, IMDB_Rating, Rotten_Tomatoes_Rating, Metacritic_Rating, ingestion_timestamp)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Iterar a través del DataFrame e insertar los registros
for index, row in df.iterrows():
    try:
        values = (
            truncate_value(row['Title'], 256), row['Year'], truncate_value(row['Rated'], 10), truncate_value(row['Released'], 50),
            truncate_value(row['Runtime'], 50), truncate_value(row['Genre'], 256), truncate_value(row['Director'], 256),
            truncate_value(row['Writer'], 256), truncate_value(row['Actors'], 512), truncate_value(row['Plot'], 1000),
            truncate_value(row['Language'], 100), truncate_value(row['Country'], 100), truncate_value(row['Awards'], 512),
            truncate_value(row['Poster'], 512), truncate_value(row['Metascore'], 10), truncate_value(row['imdbRating'], 10),
            truncate_value(row['imdbVotes'], 50), truncate_value(row['imdbID'], 50), truncate_value(row['Type'], 50),
            truncate_value(row['DVD'], 50), truncate_value(row['BoxOffice'], 100), truncate_value(row['Production'], 256),
            truncate_value(row['Website'], 256), truncate_value(row['Response'], 10), truncate_value(row['IMDB_Rating'], 10),
            truncate_value(row['Rotten_Tomatoes_Rating'], 10), truncate_value(row['Metacritic_Rating'], 10),
            row['ingestion_timestamp']
        )
        
        # Asegurarse de que el número de valores sea correcto para la consulta
        if len(values) != 28:
            print(f"Error en la cantidad de valores para el registro {index}: se esperaban 28, pero se encontraron {len(values)}")
            continue

        cur.execute(insert_query, values)
        conn.commit()
    except Exception as e:
        print(f"Error al insertar el registro {index}: {e}")

# Cerrar la conexión
cur.close()
conn.close()
print("Datos cargados exitosamente en Redshift")
