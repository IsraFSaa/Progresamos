# Proyecto ETL con Dagster

Este proyecto configura un pipeline ETL utilizando Dagster para extraer datos meteorológicos de varias ciudades, transformarlos y cargarlos en una base de datos SQLite.

## Requisitos

1. **Python**: Asegúrate de tener Python 3.12 o superior instalado.
2. **Paquetes de Python**: Usa `pip` para instalar los paquetes necesarios.

## Instalación


1. **Instala las Dependencias**

    Asegúrate de estar en el entorno virtual y ejecuta:

    ```bash
    pip install -r requirements.txt
    ```

    El archivo `requirements.txt` debería contener al menos los siguientes paquetes con sus versiones recomendadas:

    ```
    dagster==1.2.0
    dagit==1.2.0
    sqlite3==3.38.0
    pandas==2.0.3
    requests==2.31.0
    ```

## Ejecución del Pipeline

1. **Inicia Dagit**

    Desde el directorio del proyecto, ejecuta:

    ```bash
    dagit -f etl_job.py
    ```

    Esto lanzará la interfaz gráfica de Dagit en `http://localhost:3000`.

2. **Ejecuta el Job**

    - Abre tu navegador y navega a `http://localhost:3000`.
    - Selecciona el `job` que configuraste (por ejemplo, `etl_job`).
    - Haz clic en "Launch Run" para ejecutar el pipeline ETL.

## Visualización de la Base de Datos SQLite

1. **Instala DBeaver**

    Descarga e instala [DBeaver](https://dbeaver.io/), una herramienta para visualizar y gestionar bases de datos SQLite y otros sistemas de bases de datos.

2. **Conecta a la Base de Datos**

    - Abre DBeaver.
    - Crea una nueva conexión seleccionando "SQLite".
    - En el campo "Database File", selecciona el archivo `nombre_base_datos.db` que configuraste en `config.txt`.

3. **Explora las Tablas**

    Una vez conectado, podrás ver las tablas `city_catalog`, `weather`, `weather_details`, y `weather_summary`. Puedes explorar y ejecutar consultas SQL directamente desde DBeaver para verificar los datos almacenados.
