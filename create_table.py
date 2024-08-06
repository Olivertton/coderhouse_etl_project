import psycopg2

# Configuración de la conexión
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'
dbname = 'data-engineer-database'
user = 'irving_ramirez_coderhouse'
password = 'T93hbU4sqc'

# Conectar a Redshift
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cur = conn.cursor()

# Eliminar la tabla si ya existe
drop_table_query = "DROP TABLE IF EXISTS movies_2024;"
cur.execute(drop_table_query)
conn.commit()

# Crear la tabla
create_table_query = """
CREATE TABLE IF NOT EXISTS movies_2024 (
    Title VARCHAR(256),
    Year INT,
    Rated VARCHAR(10),
    Released VARCHAR(50),
    Runtime VARCHAR(50),
    Genre VARCHAR(256),
    Director VARCHAR(256),
    Writer VARCHAR(256),
    Actors VARCHAR(256),
    Plot TEXT,
    Language VARCHAR(50),
    Country VARCHAR(50),
    Awards VARCHAR(256),
    Poster VARCHAR(256),
    Metascore VARCHAR(10),
    imdbRating VARCHAR(10),
    imdbVotes VARCHAR(50),
    imdbID VARCHAR(50),
    Type VARCHAR(50),
    DVD VARCHAR(50),
    BoxOffice VARCHAR(50),
    Production VARCHAR(256),
    Website VARCHAR(256),
    Response VARCHAR(10),
    IMDB_Rating VARCHAR(10),
    Rotten_Tomatoes_Rating VARCHAR(10),
    Metacritic_Rating VARCHAR(10)
);
"""
cur.execute(create_table_query)
conn.commit()

# Cerrar la conexión
cur.close()
conn.close()
print("Tabla creada exitosamente en Redshift")