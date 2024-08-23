import sqlite3
import requests
import pandas as pd
from dagster import job, op

# Leer configuraciones desde un archivo de texto
def leer_configuracion(archivo_config):
    configuracion = {}
    with open(archivo_config, 'r') as archivo:
        for linea in archivo:
            clave, valor = linea.strip().split('=')
            configuracion[clave] = valor
    return configuracion

config = leer_configuracion('config.txt')

@op
def crear_tablas():
    # Configuración de la base de datos SQLite
    conn = sqlite3.connect(config['db_name'])
    cursor = conn.cursor()

    # Crear tablas si no existen
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_catalog (
        city_id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_name TEXT UNIQUE NOT NULL,
        country TEXT NOT NULL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        temperature REAL,
        humidity INTEGER,
        wind_speed REAL,
        FOREIGN KEY(city_id) REFERENCES city_catalog(city_id),
        UNIQUE(city_id, date)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        pressure INTEGER,
        visibility INTEGER,
        FOREIGN KEY(city_id) REFERENCES city_catalog(city_id),
        UNIQUE(city_id, date)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        summary TEXT,
        FOREIGN KEY(city_id) REFERENCES city_catalog(city_id),
        UNIQUE(city_id, date)
    )
    ''')

    conn.commit()
    conn.close()

@op
def obtener_datos_ciudad():
    ciudades = ['London', 'Mexico City', 'New York']
    api_key = config['api_key']
    data = [obtener_datos_ciudad_individual(api_key, ciudad) for ciudad in ciudades]
    df = pd.DataFrame(data)
    return df

def obtener_datos_ciudad_individual(api_key, city):
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
    respuesta = requests.get(url)
    datos = respuesta.json()
    return {
        'city_name': city,
        'country': datos['sys']['country'],
        'date': pd.to_datetime('now').strftime('%Y-%m-%d %H:%M:%S'),
        'temperature': datos['main']['temp'],
        'humidity': datos['main']['humidity'],
        'wind_speed': datos['wind']['speed'],
        'pressure': datos['main']['pressure'],
        'visibility': datos['visibility'],
        'summary': datos['weather'][0]['description']
    }

@op
def limpiar_transformar_datos(df):
    # Limpiar datos
    df = limpiar_datos(df)
    
    return df

def limpiar_datos(df):
    for col in df.select_dtypes(include=['float', 'int']).columns:
        df[col] = df[col].fillna(0)  # Reemplaza NaN con 0
    return df


@op
def cargar_datos(df):
    conn = sqlite3.connect(config['db_name'])
    cursor = conn.cursor()

    for _, fila in df.iterrows():
        # Insertar o actualizar el catálogo de ciudades
        cursor.execute('''
        INSERT OR IGNORE INTO city_catalog (city_name, country)
        VALUES (?, ?)
        ''', (fila['city_name'], fila['country']))

        cursor.execute('SELECT city_id FROM city_catalog WHERE city_name = ?', (fila['city_name'],))
        city_id = cursor.fetchone()[0]

        # Insertar o actualizar datos en la tabla weather
        cursor.execute('''
        INSERT INTO weather (city_id, date, temperature, humidity, wind_speed) 
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(city_id, date) 
        DO UPDATE SET 
        temperature=excluded.temperature, 
        humidity=excluded.humidity, 
        wind_speed=excluded.wind_speed
        ''', (city_id, fila['date'], fila['temperature'], fila['humidity'], fila['wind_speed']))

        # Insertar o actualizar datos en la tabla weather_details
        cursor.execute('''
        INSERT INTO weather_details (city_id, date, pressure, visibility) 
        VALUES (?, ?, ?, ?)
        ON CONFLICT(city_id, date) 
        DO UPDATE SET 
        pressure=excluded.pressure, 
        visibility=excluded.visibility
        ''', (city_id, fila['date'], fila['pressure'], fila['visibility']))

        # Insertar o actualizar datos en la tabla weather_summary
        cursor.execute('''
        INSERT INTO weather_summary (city_id, date, summary) 
        VALUES (?, ?, ?)
        ON CONFLICT(city_id, date) 
        DO UPDATE SET 
        summary=excluded.summary
        ''', (city_id, fila['date'], fila['summary']))

    conn.commit()
    conn.close()

@job
def etl_job():
    crear_tablas()
    raw_data = obtener_datos_ciudad()
    transformed_data = limpiar_transformar_datos(raw_data)
    cargar_datos(transformed_data)
