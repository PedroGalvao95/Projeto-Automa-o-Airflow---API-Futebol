from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import json

DADO_BRONZE = Dataset("postgres://bronze_players")
DADO_SILVER = Dataset("postgres://silver_players")

default_args = {
    'owner': 'Projeto Dados Futebol',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#definição da dag para camada silver
@dag(
    dag_id='futebol_silver_transformation',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=[DADO_BRONZE], 
    catchup=False,
    tags=['silver', 'futebol']
)
def futebol_silver():

    @task(outlets=[DADO_SILVER])
    #transformação dos dados brutos da camada bronze para a camada silver (estruturada)
    def transform_to_silver():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 1. Cria a tabela Silver (Estruturada)
        create_silver_table = """
            CREATE TABLE IF NOT EXISTS silver_players (
                player_id INT PRIMARY KEY,
                name VARCHAR(255),      -- Nome
                team VARCHAR(255),      -- Time
                position VARCHAR(50),   -- Posição
                appearances INTEGER,    -- Aparições
                minutes INTEGER,        -- Tempo de campo
                goals INTEGER,          -- Gols marcados
                assists INTEGER,        -- Assistências
                shots_total INTEGER,    -- Chutes totais
                passes_key INTEGER,     -- Passes decisivos
                passes_total INTEGER,       -- Passes totais
                fouls_committed INTEGER, -- Faltas cometidas
                touches INTEGER,          -- Toques na bola
                rating FLOAT,            -- Avaliação geral
                dribbles_attempts INTEGER,  -- Tentativas de drible
                dribbles_success INTEGER,   -- Dribles bem-sucedidos
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        pg_hook.run(create_silver_table)

        # 2. Busca os dados brutos da Bronze (que já estão lá no banco)
        records = pg_hook.get_records("SELECT data_bruta FROM bronze_players")
        
        if not records:
            print("Nenhum dado encontrado na Bronze para processar.")
            return

        print(f"Processando {len(records)} registros da Bronze...")

        for record in records:
            item = record[0]
            player = item.get('player', {})
            # Pegamos o primeiro item da lista de estatísticas
            stats = item.get('statistics', [{}])[0] 

            insert_query = """
                INSERT INTO silver_players (player_id, name, team, position, appearances, minutes, 
                    goals, assists, shots_total, passes_key, passes_total, 
                    fouls_committed, touches, rating, dribbles_attempts, dribbles_success)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    goals = EXCLUDED.goals,
                    assists = EXCLUDED.assists,
                    rating = EXCLUDED.rating,
                    updated_at = CURRENT_TIMESTAMP;
            """
            
            values = (
                player.get('id'),                                   
                player.get('name'),                                 
                stats.get('team', {}).get('name'),                 
                stats.get('games', {}).get('position'),             
                stats.get('games', {}).get('appearences') or 0,     
                stats.get('games', {}).get('minutes') or 0,         
                stats.get('goals', {}).get('total') or 0,          
                stats.get('goals', {}).get('assists') or 0,         
                stats.get('shots', {}).get('total') or 0,           
                stats.get('passes', {}).get('key') or 0,            
                stats.get('passes', {}).get('total') or 0,          
                stats.get('fouls', {}).get('committed') or 0,       
                stats.get('games', {}).get('touches') or 0,         
                stats.get('games', {}).get('rating'),                
                stats.get('dribbles', {}).get('attempts') or 0,    
                stats.get('dribbles', {}).get('success') or 0      
            )
            pg_hook.run(insert_query, parameters=values)
        
        print("Transformação concluída!")

    transform_to_silver()

futebol_silver_dag = futebol_silver()