import requests
import pandas as pd

# Clave API
api_key = '2d55e958'

# Término de búsqueda genérico
search_term = 'love'

# Función para obtener el valor de los ratings de diferentes fuentes
def get_ratings(ratings, source):
    for rating in ratings:
        if rating['Source'] == source:
            return rating['Value']
    return None

# Inicializar la lista para almacenar los detalles de las películas
movies_list = []

# Iterar a través de las páginas de resultados
for page in range(1, 11):  # Buscar solo 100 resultados
    # URL de la API marcando pelicula, json y el término en el titulo
    search_url = f'http://www.omdbapi.com/?apikey={api_key}&s={search_term}&type=movie&r=json&page={page}'

    # Hacer una solicitud GET a la API
    search_response = requests.get(search_url)

    # Imprimir la URL y el estado de la respuesta
    print(f"URL: {search_url}")
    print(f"Estado de la respuesta: {search_response.status_code}")

    # Verificar que la solicitud fue exitosa
    if search_response.status_code == 200:
        search_data = search_response.json()
        # Imprimir los datos recibidos
        print(f"Datos recibidos: {search_data}")

        if 'Search' in search_data:
            for movie in search_data['Search']:
                movie_id = movie['imdbID']
                # Hacer una solicitud adicional para obtener detalles de la película
                details_url = f'http://www.omdbapi.com/?apikey={api_key}&i={movie_id}&r=json'
                details_response = requests.get(details_url)
                if details_response.status_code == 200:
                    movie_details = details_response.json()
                    # Extraer ratings de diferentes fuentes
                    movie_details['IMDB_Rating'] = get_ratings(movie_details.get('Ratings', []), 'Internet Movie Database')
                    movie_details['Rotten_Tomatoes_Rating'] = get_ratings(movie_details.get('Ratings', []), 'Rotten Tomatoes')
                    movie_details['Metacritic_Rating'] = get_ratings(movie_details.get('Ratings', []), 'Metacritic')
                    movies_list.append(movie_details)
                else:
                    print(f"Error al acceder a los detalles de la película: {movie_id}")
        else:
            print("No se encontraron más películas")
            break
    else:
        print("Error al acceder a la API")
        break

# Convertir datos a un DataFrame de pandas
df = pd.DataFrame(movies_list)
# Guardar los datos en un archivo CSV
df.to_csv('movies_2024.csv', index=False)
print("Datos detallados guardados en movies_2024.csv")
