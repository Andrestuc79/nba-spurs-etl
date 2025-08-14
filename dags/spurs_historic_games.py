from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from nba_api.stats.endpoints import teamgamelogs
from nba_api.stats.static import teams
from psycopg2 import extras
import psycopg2
import time
import pandas as pd
import io
from datetime import datetime
import requests
import json
import os
import random


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 1),
    "retries": 1
}

#Datos de Conexión y Configuración
#TEAM_NAME = "San Antonio Spurs"
#YEARS_BACK = 10
LOCAL_DIR = "/tmp/spurs_games/"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET_NAME = "nba-data"

BRONZE_PATH = "/tmp/bronze"
MINIO_BUCKET = "nba-bronze"
API_NBA = "https://api-nba-endpoint.example"
API_MARKET = "https://api-nba-market.example"

START_YEAR = 2015  # Cambiá esto para limitar desde hace cuántos años querés datos
CURRENT_YEAR = datetime.now().year


POSTGRES_HOST = "postgres"
POSTGRES_DB = "nba"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_PORT = 5432

TABLE_PREFIX = "silver"
#TABLE_NAME = "silver.spurs_games"
#endregion

#Toma de datos api nba

with DAG(
    "spurs_full_pipeline_filtered",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["nba", "spurs", "etl", "market"]
):

    @task
    def extract(endpoint: str, filename: str, api_base: str, start_year: int, end_year: int):
        os.makedirs(BRONZE_PATH, exist_ok=True)
        
        # Si la API soporta filtros de fecha, agregarlos al request
        params = {
            "start_year": start_year,
            "end_year": end_year
        }
        
        resp = requests.get(f"{api_base}/{endpoint}", params=params)
        data = resp.json()

        path = os.path.join(BRONZE_PATH, filename)
        with open(path, "w") as f:
            json.dump(data, f)

        return path

    @task
    def generate_market_json(players_file: str):
        # Carga los jugadores reales
        with open(players_file, "r") as f:
            players = json.load(f)
        if isinstance(players, dict) and "results" in players:
            players = players["results"]
        elif isinstance(players, dict):
            players = list(players.values())

        # Si hay menos de 500 jugadores, repite la lista
        while len(players) < 500:
            players += players
        players = players[:500]

        # Free agents
        free_agents = []
        for p in random.sample(players, 500):
            free_agents.append({
                "player_id": p.get("PLAYER_ID", p.get("id", random.randint(1000, 9999))),
                "name": p.get("PLAYER_NAME", p.get("full_name", "Unknown")),
                "position": random.choice(["PG", "SG", "SF", "PF", "C"]),
                "age": random.randint(20, 38),
                "status": "Free Agent"
            })

        # Salaries
        salaries = []
        for p in random.sample(players, 500):
            salaries.append({
                "player_id": p.get("PLAYER_ID", p.get("id", random.randint(1000, 9999))),
                "name": p.get("PLAYER_NAME", p.get("full_name", "Unknown")),
                "salary": random.randint(900000, 45000000),
                "season": f"{CURRENT_YEAR-1}-{str(CURRENT_YEAR)[-2:]}"
            })

        # Injuries
        injuries = []
        for p in random.sample(players, 500):
            injuries.append({
                "player_id": p.get("PLAYER_ID", p.get("id", random.randint(1000, 9999))),
                "name": p.get("PLAYER_NAME", p.get("full_name", "Unknown")),
                "injury": random.choice(["Knee", "Ankle", "Hamstring", "Back", "Shoulder"]),
                "status": random.choice(["Out", "Day-to-day", "Questionable"]),
                "expected_return": str(datetime(CURRENT_YEAR, random.randint(1, 12), random.randint(1, 28)).date())
            })

        # Guardar los archivos
        os.makedirs(BRONZE_PATH, exist_ok=True)
        files = []
        for fname, data in [
            ("free_agents.json", free_agents),
            ("salaries.json", salaries),
            ("injuries.json", injuries)
        ]:
            path = os.path.join(BRONZE_PATH, fname)
            with open(path, "w") as f:
                json.dump(data, f)
            files.append(path)
        return files 

    @task
    def combine_bronze_files(games, players, teams, market_files):
        # market_files ya es una lista de paths
        return [games, players, teams] + market_files

    @task_group
    def bronze_layer():
        games = extract("games/spurs", "games.json", API_NBA, START_YEAR, CURRENT_YEAR)
        players = extract("players/spurs", "players.json", API_NBA, START_YEAR, CURRENT_YEAR)
        teams = extract("teams", "teams.json", API_NBA, START_YEAR, CURRENT_YEAR)
        market_files = generate_market_json(players)
        all_files = combine_bronze_files(games, players, teams, market_files)
        return all_files
    
#endregion
     
#Subida a MinIO

    @task
    def upload_to_minio(files):
        hook = S3Hook(aws_conn_id="minio_conn")
        for f in files:
            hook.load_file(
                filename=f,
                key=os.path.basename(f),
                bucket_name=MINIO_BUCKET,
                replace=True
         )
        print(f"✅ Subidos {len(files)} archivos a MinIO")
#endregion

#Inserción en PostgreSQL
    @task
    def insert_into_postgres(file_path: str):
        """Inserta un archivo JSON en PostgreSQL"""
        table_name = f"{TABLE_PREFIX}.{os.path.splitext(os.path.basename(file_path))[0]}"
        
        with open(file_path, "r") as f:
            data = json.load(f)

        # Normalizamos a DataFrame
        if isinstance(data, dict) and "results" in data:
            df = pd.DataFrame(data["results"])
        else:
            df = pd.DataFrame(data)

        if df.empty:
            print(f"⚠ No hay datos para insertar en {table_name}")
            return

        # Conexión e inserción
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()

        cols = list(df.columns)
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(cols)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """

        data_tuples = [tuple(x) for x in df.to_numpy()]
        extras.execute_values(cursor, insert_query, data_tuples)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Insertados {len(df)} registros en {table_name}")

    
#endregion

#DBT a Gold

    @task_group
    def gold_layer():
        return BashOperator(
            task_id="run_dbt_gold",
            bash_command="cd /usr/local/airflow/dbt && dbt run --select gold"
        )
#endregion

    bronze_data = bronze_layer()
    upload = upload_to_minio(bronze_data)
    silver = insert_into_postgres.expand(file_path=bronze_data)
    gold = gold_layer()

    bronze_data >> upload >> silver >> gold

    