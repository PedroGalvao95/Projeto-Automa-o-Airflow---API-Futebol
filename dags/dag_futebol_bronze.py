from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import requests
import json
import time

DADO_BRONZE = Dataset("postgres://bronze_players")

# IDs dos 20 times da Premier League 2024
PREMIER_LEAGUE_TEAMS = [
    33,  # Manchester United
    34,  # Newcastle
    35,  # Bournemouth
    36,  # Fulham
    38,  # Wolverhampton
    39,  # Wolves (reserva)
    40,  # Liverpool
    41,  # Southampton
    42,  # Arsenal
    45,  # Everton
    46,  # Leicester
    47,  # Tottenham
    48,  # West Ham
    49,  # Chelsea
    50,  # Manchester City
    51,  # Brighton
    52,  # Crystal Palace
    55,  # Brentford
    65,  # Nottingham Forest
    66,  # Aston Villa
]

default_args = {
    'owner': 'Projeto Dados Futebol',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='futebol_bronze_extraction',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['bronze', 'futebol']
)
def futebol_bronze():

    @task
    def extract_all_players():
        api_key = Variable.get("api_football_key")
        headers = {
            "x-apisports-key": api_key,
            "x-apisports-host": "v3.football.api-sports.io"
        }

        all_players = []
        seen_ids = set()  # evita duplicatas

        for team_id in PREMIER_LEAGUE_TEAMS:
            print(f"Iniciando extração do time: {team_id}")
            page = 1
            while True:
                url = "https://v3.football.api-sports.io/players"
                params = {
                    "team": str(team_id),
                    "season": "2024",  # Mantenha a temporada
                    "page": str(page)
                }

                response = requests.get(url, headers=headers, params=params)
                data = response.json()

                print(f"Time {team_id} | Página {page} | Status: {response.status_code} | Resultados: {data.get('results', 0)}")

                if data.get('errors'):
                    print(f"Erro na API: {data['errors']}")
                    break

                players = data.get('response', [])
                if not players:
                    print(f"Finalizado: Time {team_id} não tem mais jogadores na página {page}")
                    break

                for player in players:
                    pid = player.get('player', {}).get('id')
                    if pid and pid not in seen_ids:
                        seen_ids.add(pid)
                        all_players.append(player)

                # Verifica se há mais páginas
                paging = data.get('paging', {})
                if paging.get('current', 1) >= paging.get('total', 1):
                    break

                page += 1
                time.sleep(7)  # respeita o rate limit da API gratuita

            time.sleep(3)  # pausa entre times

        print(f"Total de jogadores únicos extraídos: {len(all_players)}")
        return all_players

    @task(outlets=[DADO_BRONZE])
    def load_to_bronze(data):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Cria tabela bronze se não existir
        create_table_query = """
            CREATE TABLE IF NOT EXISTS bronze_players (
                id SERIAL PRIMARY KEY,
                data_bruta JSONB,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        pg_hook.run(create_table_query)

        # Limpa dados antigos antes de inserir novos
        pg_hook.run("TRUNCATE TABLE bronze_players;")

        insert_query = "INSERT INTO bronze_players (data_bruta) VALUES (%s)"
        for item in data:
            pg_hook.run(insert_query, parameters=(json.dumps(item),))

        print(f"{len(data)} jogadores carregados na Bronze!")

    dados_brutos = extract_all_players()
    load_to_bronze(dados_brutos)

futebol_bronze_dag = futebol_bronze()