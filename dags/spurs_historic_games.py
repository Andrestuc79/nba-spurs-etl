from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from nba_api.stats.endpoints import teamgamelogs
from nba_api.stats.endpoints import playergamelogs
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.static import teams, players 
from tenacity import retry, wait_exponential, stop_after_attempt
from nba_api.stats.library.parameters import SeasonType, PlayerOrTeam
from botocore.exceptions import ClientError
from psycopg2 import extras
import psycopg2
import json
import boto3 
import os
import time
import pandas as pd
import io
import random

#region Datos de Conexión y Configuración
TEAM_NAME = "San Antonio Spurs"
#YEARS_BACK = 10
LOCAL_DIR = "/tmp/spurs_files/"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET_NAME = "nba-data"

POSTGRES_HOST = "postgres"
POSTGRES_DB = "nba"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_PORT = 5432

# Mapeo de columnas para renombrar de forma explícita
# Las claves son los nombres originales del JSON y los valores son los nuevos nombres
COLUMN_MAPPINGS = {
    'season_year': 'season',  # Para spurs_games
    'SEASON': 'season'
}

#TABLE_NAME_GAMES = "silver.spurs_games"
#endregion

#region Toma de datos api nba
def fetch_teams():
    """Obtiene todos los equipos activos en los últimos 10 años y los guarda en MinIO."""
    current_year = datetime.now().year
    start_year = current_year - 1

    all_teams = teams.get_teams()
    recent_teams = []

    # Filtramos por equipos que hayan tenido actividad en los últimos 10 años
    for t in all_teams:
        try:
            # Verificamos si el equipo tiene roster en alguna temporada en el rango
            for year in range(start_year, current_year + 1):
                roster = commonteamroster.CommonTeamRoster(team_id=t['id'], season=str(year)).get_dict()
                if roster['resultSets'][0]['rowSet']:
                    recent_teams.append(t)
                    break
        except Exception:
            continue

    local_file = os.path.join(LOCAL_DIR, "teams.json")
    os.makedirs(LOCAL_DIR, exist_ok=True)
    with open(local_file, "w") as f:
        json.dump(recent_teams, f, indent=4)

    #upload_to_minio_local(local_file, "teams_last_10_years.json")
    #print(f"✅ Guardados {len(recent_teams)} equipos en los últimos 10 años.")

def fetch_players():
    """Obtiene todos los jugadores que hayan estado en equipos activos en los últimos 10 años."""
    current_year = datetime.now().year
    start_year = current_year - 1

    # Cargamos equipos recientes
    with open(os.path.join(LOCAL_DIR, "teams.json"), "r") as f:
        recent_teams = json.load(f)

    all_players_data = []

    for t in recent_teams:
        team_id = t['id']
        for year in range(start_year, current_year + 1):
            try:
                roster = commonteamroster.CommonTeamRoster(team_id=team_id, season=str(year)).get_dict()
                rows = roster['resultSets'][0]['rowSet']
                headers = roster['resultSets'][0]['headers']
                for r in rows:
                    player_data = dict(zip(headers, r))
                    player_data["season"] = year
                    all_players_data.append(player_data)
            except Exception:
                continue

    local_file = os.path.join(LOCAL_DIR, "players.json")
    with open(local_file, "w") as f:
        json.dump(all_players_data, f, indent=4)

    #upload_to_minio_local(local_file, "players_last_10_years.json")
    #print(f"✅ Guardados {len(all_players_data)} jugadores en los últimos 10 años.")

def fetch_games():
    if not os.path.exists(LOCAL_DIR):
        os.makedirs(LOCAL_DIR)

    team_id = 1610612759  # San Antonio Spurs
    current_year = datetime.now().year
    last_season = f"{current_year - 1}-{str(current_year)[-2:]}"  # Ej: 2024-25

    print(f"Fetching season {last_season}")
    try:
        games = teamgamelogs.TeamGameLogs(
          #  team_id_nullable=team_id,
            season_nullable=last_season,
            season_type_nullable='Regular Season'
        ).get_data_frames()[0]
        time.sleep(3)  # Evitar bloqueos por rate limit
    except Exception as e:
        print(f"Error fetching {last_season}: {e}")
        return

    if games.empty:
        print(f"No data for season {last_season}, skipping.")
        return  # No guardamos archivo vacío

    file_path = os.path.join(LOCAL_DIR, f"games.json")
    games.to_json(file_path, orient="records")
    print(f"✅ Saved {len(games)} records for season {last_season}")

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def fetch_all_player_game_logs(season_year, season_type='Regular Season'):
    """
    Obtiene los registros de partidos de todos los jugadores para una temporada completa.
    
    season_year: El año de la temporada, por ejemplo '2023-24'.
    season_type: 'Regular Season', 'Playoffs', 'Pre Season', etc.
    """
    print(f"Buscando estadísticas de la temporada {season_year} ({season_type})...")
    
    # Se usa el endpoint correcto: playergamelogs
    game_logs = playergamelogs.PlayerGameLogs(
        season_nullable=season_year,
        season_type_nullable=season_type
    )
    return game_logs.get_dict()

def fetch_player_stats_by_game():
    """
    Obtiene las estadísticas de los jugadores a nivel de temporada y las guarda en un archivo local.
    """
    season_year = "2024-25" 
    
    try:
        player_stats_data = fetch_all_player_game_logs(season_year, season_type='Regular Season')
        
        if not player_stats_data or 'resultSets' not in player_stats_data or len(player_stats_data['resultSets']) == 0:
            raise ValueError("La respuesta de la API no contiene datos válidos.")
            
        rows = player_stats_data['resultSets'][0]['rowSet']
        headers = player_stats_data['resultSets'][0]['headers']
        
        all_stats = []
        for r in rows:
            stat_record = dict(zip(headers, r))
            all_stats.append(stat_record)
        
        print(f"✅ Se obtuvieron {len(all_stats)} registros de estadísticas para la temporada {season_year}.")
        
        local_file = os.path.join(LOCAL_DIR, f"player_stats_by_game.json")
        with open(local_file, "w") as f:
            json.dump(all_stats, f, indent=4)
        print(f"✅ Archivo guardado en {local_file}")

    except Exception as e:
        print(f"⚠ Error al procesar las estadísticas de la temporada {season_year}: {e}")

def generate_fake_salaries():
    """Genera salarios ficticios para todos los jugadores."""
    players_file = os.path.join(LOCAL_DIR, "players.json")
    if not os.path.exists(players_file):
        print("❌ No se encontró players.json.")
        return

    with open(players_file, "r") as f:
        players_data = json.load(f)

    salaries_data = []
    for player in players_data:
        salary = round(random.uniform(500000, 45000000), 2)  # Entre 0.5M y 45M USD
        salaries_data.append({
            "PLAYER_ID": player.get("PLAYER_ID"),
            "PLAYER_NAME": player.get("PLAYER"),
            "SEASON": player.get("season"),
            "SALARY_USD": salary
        })

    local_file = os.path.join(LOCAL_DIR, "salaries.json")
    with open(local_file, "w") as f:
        json.dump(salaries_data, f, indent=4)
    print(f"✅ Generados salarios ficticios para {len(salaries_data)} jugadores.")

def generate_free_agents():
    players_file = os.path.join(LOCAL_DIR, "players.json")
    if not os.path.exists(players_file):
        print("❌ No se encontró players.json.")
        return

    with open(players_file, "r") as f:
        players_data = json.load(f)


    # Normalizar claves a minúsculas
    players_data = [
        {k.lower(): v for k, v in player.items()}
        for player in players_data
    ]

    unique_players_dict = {player['player_id']: player for player in players_data}

    # Convertimos los valores del diccionario de nuevo a una lista
    unique_players = list(unique_players_dict.values())

    # Tomamos una muestra aleatoria de la lista de jugadores únicos
    free_agents_sample = random.sample(unique_players, min(200, len(unique_players)))
    
    free_agents_data = []
    for player in free_agents_sample:
        free_agents_data.append({
            "PLAYER_ID": player.get("player_id"),
            "PLAYER_NAME": player.get("player"),
            "POSITION": player.get("position"),
            "AGE": random.randint(19, 38),
            "AGE_EXPERIENCE": random.randint(1, 15),
            "AVALAIBLEFROM": f"{random.randint(2020, 2025)}-07-01"
        })

    local_file = os.path.join(LOCAL_DIR, "free_agents.json")
    with open(local_file, "w") as f:
        json.dump(free_agents_data, f, indent=4)
    print(f"✅ Generados {len(free_agents_data)} agentes libres ficticios.")

def generate_injuries():

    players_file = os.path.join(LOCAL_DIR, "players.json")
    if not os.path.exists(players_file):
        print("❌ No se encontró players.json.")
        return

    with open(players_file, "r") as f:
        players_data = json.load(f)

    lesiones_catalogo = [
        "Esguince de tobillo leve - requerirá reposo de 1 a 2 semanas",
        "Desgarro muscular en el cuádriceps - baja estimada de 4 semanas",
        "Fractura en el metacarpo de la mano derecha - recuperación de 6 semanas",
        "Contusión en la rodilla izquierda - tratamiento diario y control semanal",
        "Lesión en el manguito rotador - rehabilitación de 3 meses",
        "Lumbalgia aguda - reposo deportivo por 10 días",
        "Tendinitis rotuliana - tratamiento fisioterapéutico de 6 semanas",
        "Rotura del ligamento cruzado anterior - fuera de la temporada",
        "Esguince cervical - control y reposo por 2 semanas",
        "Conmoción cerebral - protocolo de recuperación mínimo 7 días"
    ]

    injuries_data = []
    for player in random.sample(players_data, min(200, len(players_data))):
        injuries_data.append({
            "PLAYER_ID": player.get("PLAYER_ID"),
            "PLAYER_NAME": player.get("PLAYER"),
            "LESION": random.choice(lesiones_catalogo),
            "DATE": f"{random.randint(2020, 2025)}-{random.randint(1,12):02}-{random.randint(1,28):02}"
        })

    local_file = os.path.join(LOCAL_DIR, "injuries.json")
    with open(local_file, "w") as f:
        json.dump(injuries_data, f, indent=4)
    print(f"✅ Generadas {len(injuries_data)} lesiones ficticias.")


#endregion
     
#region Subida a MinIO

def upload_files_to_minio():
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # Verifica si el bucket existe y lo crea si no.
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET_NAME)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Bucket '{MINIO_BUCKET_NAME}' no encontrado. Creándolo...")
            s3_client.create_bucket(Bucket=MINIO_BUCKET_NAME)
        else:
            print(f"Error inesperado al verificar el bucket: {e}")
            return
            
    # Itera sobre los archivos y los sube, sobrescribiendo si ya existen.
    for filename in os.listdir(LOCAL_DIR):
        file_path = os.path.join(LOCAL_DIR, filename)
        file_key = f"spurs/{filename}"
        
        s3_client.upload_file(file_path, MINIO_BUCKET_NAME, file_key)
        print(f"✅ Archivo '{filename}' subido/reemplazado en MinIO en '{file_key}'")

#endregion

#region Inserción en PostgreSQL

def drop_table(table_name):
    """Elimina una tabla si existe."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(host=POSTGRES_HOST, database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, port=POSTGRES_PORT)
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        drop_table_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        cur.execute(drop_table_query)
        print(f"✅ Tabla {table_name} eliminada (si existía).")
    except Exception as e:
        print(f"ERROR - ❌ Fallo al eliminar la tabla {table_name}: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def insert_json_to_postgres(key, table, pk):
    """Inserta datos de un archivo JSON desde MinIO a una tabla de Postgres."""
    conn = None
    cur = None
    try:
        s3 = boto3.client('s3',
                          endpoint_url=f"http://{MINIO_ENDPOINT}",
                          aws_access_key_id=MINIO_ACCESS_KEY,
                          aws_secret_access_key=MINIO_SECRET_KEY,
                          config=boto3.session.Config(signature_version='s3v4'))

        obj = s3.get_object(Bucket=MINIO_BUCKET_NAME, Key=key)
        data = json.loads(obj['Body'].read().decode('utf-8'))

        if not data:
            print(f"INFO - ⚠️ El archivo {key} está vacío. No se insertarán registros.")
            return

        sanitized_data = []
        for record in data:
            sanitized_record = {}
            for original_key, value in record.items():
                sanitized_key = original_key.lower()
                new_key = COLUMN_MAPPINGS.get(sanitized_key, sanitized_key)
                sanitized_record[new_key] = value
            sanitized_data.append(sanitized_record)

        columns_seen = set()
        sanitized_columns = []
        if sanitized_data:
            for col in sanitized_data[0].keys():
                if col not in columns_seen:
                    sanitized_columns.append(col)
                    columns_seen.add(col)

        column_definitions = ", ".join([f'"{col}" VARCHAR' for col in sanitized_columns])
        
        if pk:
            if isinstance(pk, list):
                pk_string = ", ".join([f'"{p}"' for p in pk])
            else:
                pk_string = f'"{pk}"'
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table} ({column_definitions}, PRIMARY KEY ({pk_string}));"
        else:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table} ({column_definitions});"


      #  if isinstance(pk, list):
       #     pk_string = ", ".join([f'"{p}"' for p in pk])
      #  else:
       #     pk_string = f'"{pk}"'
            
       # create_table_query = f"CREATE TABLE IF NOT EXISTS {table} ({column_definitions}, PRIMARY KEY ({pk_string}));"

        conn = psycopg2.connect(host=POSTGRES_HOST, database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, port=POSTGRES_PORT)
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        cur.execute(create_table_query)
        print(f"✅ Tabla {table} creada con columnas: {sanitized_columns}")

        rows = []
        for record in sanitized_data:
            row_values = [record.get(col) for col in sanitized_columns]
            rows.append(row_values)

        insert_query = f"INSERT INTO {table} ({', '.join(f'"{c}"' for c in sanitized_columns)}) VALUES %s"

        extras.execute_values(cur, insert_query, rows)
        print(f"✅ Insertados {len(rows)} registros en {table}")

    except Exception as e:
        print(f"ERROR - ❌ Fallo al insertar datos: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def insert_all_files():
    """Procesa todos los JSON en MinIO y los inserta en sus tablas."""
    mapping = {
        "spurs/games.json": ("silver.games", None),
        "spurs/teams.json": ("silver.teams", "id"),
        "spurs/players.json": ("silver.players", ['player_id', 'season']),
        "spurs/salaries.json": ("silver.salaries", ['player_id', 'season']),
        "spurs/free_agents.json": ("silver.free_agents", 'player_id'),
        "spurs/injuries.json": ("silver.injuries", None),
        "spurs/player_stats_by_game.json": ("silver.player_stats", None)
    }

    for key, (table, pk) in mapping.items():
        # Llama a la función de borrado de tabla para cada elemento del mapping
        drop_table(table)
        insert_json_to_postgres(key, table, pk)
#endregion

#region DAG
with DAG(
    dag_id="spurs_historic_games",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["nba", "spurs", "minio"],
) as dag:

    #with TaskGroup(group_id="bronze_tg") as bronze_tg:

        # task_fetch_all_teams = PythonOperator(
        #      task_id="fetch_teams",
        #      python_callable=fetch_teams
        #  )

        # task_fetch_players = PythonOperator(
        #      task_id="fetch_players",
        #      python_callable=fetch_players
        # )       

        # task_fetch_games = PythonOperator(
        #      task_id="fetch_spurs_games",
        #      python_callable=fetch_games
        #  )

        # task_fetch_player_stats = PythonOperator(
        #      task_id="fetch_player_stats_by_game",
        #      python_callable=fetch_player_stats_by_game
        #  )

        # task_generate_salaries = PythonOperator(
        #      task_id="generate_fake_salaries",
        #      python_callable=generate_fake_salaries
        #  )

        #task_generate_free_agents = PythonOperator(
        #       task_id="generate_free_agents",
        #      python_callable=generate_free_agents
        #)

        # task_generate_injuries = PythonOperator(
        #      task_id="generate_injuries",
        #      python_callable=generate_injuries
        #  )

        #task_upload_to_minio = PythonOperator(
        #     task_id="upload_to_minio",
        #     python_callable=upload_files_to_minio
        #)


        #task_fetch_all_teams >> task_fetch_players >> task_fetch_games
        #task_fetch_games >> task_fetch_player_stats
        #task_fetch_player_stats >> [task_generate_salaries, task_generate_free_agents, task_generate_injuries]
        #[task_generate_salaries, task_generate_free_agents, task_generate_injuries] >> 
        
        #task_upload_to_minio

      
        #task_generate_free_agents >> task_upload_to_minio

    #with TaskGroup(group_id="silver_tg") as silver_tg:

    #    task_insert_games = PythonOperator(
    #        task_id="insert_in_postgres",
    #        python_callable=insert_all_files
    #    )

    with TaskGroup(group_id="gold_tg") as gold_tg:

        dbt_run = BashOperator(
            task_id="run_dbt_models",
            bash_command="cd /usr/local/airflow/dbt && dbt run"
        )

    #bronze_tg >> 
    #silver_tg >> 
    gold_tg

    #endregion