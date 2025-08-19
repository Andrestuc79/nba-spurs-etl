# NBA Spurs ETL Pipeline

![alt text](image-1.png)

Este proyecto implementa un pipeline completo de **Extracción, Carga y Transformación (ETL/ELT)** para datos de los *San Antonio Spurs*, usando una arquitectura moderna con:

- **Apache Airflow** para orquestar el flujo (Bronze → Silver → Gold)  
- **MinIO** como almacenamiento de archivos **Bronze** (JSON)  
- **PostgreSQL** para la capa **Silver** (raw)  
- **dbt** para transformaciones analíticas (capa **Gold**)  
- **Superset** como capa de visualización BI

---

###  Arquitectura general


![alt text](image.png)


**Descripción del flujo**:

1. **Bronze (Raw data)**  
   - DAG en Airflow que:
     - Consume la API de la NBA (`nba_api`)  
     - Genera JSON: `games.json`, `players.json`, `teams.json`, `salaries.json`, etc.  
     - Almacena esos archivos en MinIO

2. **Silver (Cleaning / Staging)**  
   - Descarga los JSON de MinIO  
   - Inserta los datos en tablas PostgreSQL sin transformación  
   - El modelo raw queda listo para análisis y transformación

3. **Gold (Analytics / BI)**  
   - Usa **dbt** para aplicar lógica:
      -Analiza el rendimiento de los San Antonio Spurs en diferentes métricas clave (como el porcentaje de tiro, rebotes y robos). Luego, compara estos valores con los promedios de la liga y los del mejor equipo para cada temporada y de esta manera, se
      muestran las debilidades y fortalezas del equipo en última temporada.
      -Se usa los datos de rendimientos para identificar las principales debilidades del equipo. Luego, busca jugadores que destaquen en las métricas relacionadas con esas debilidades y se asocian datos como su rendimiento en la última temporada, salario,
      lesiones si es agente libre.
      -Se roporciona un resumen que incluye el número de victorias, derrotas, el total de partidos jugados, el promedio de puntos anotados, las rachas del equipo, los resultados contra sus rivales y el ranking del equipo en la liga.
      -Se detalla la contribución individual de cada jugador de los Spurs en sus rubros claves.
      -Compara el rendimiento del equipo en partidos jugados como local (en casa) frente a los jugados como visitante (fuera de casa).

   - Produce tablas analíticas en `schema: gold` dentro de Postgres

4. **Visualización / Reporting** (opcional)  
   - Conectar Superset a la base Postgres `gold`  
   - Crear dashboards accesibles vía web

---

###  Primeros pasos (Getting Started)

1. Clonar el repositorio  
   ```bash
   git clone https://github.com/Andrestuc79/nba-spurs-etl.git
   cd nba-spurs-etl

