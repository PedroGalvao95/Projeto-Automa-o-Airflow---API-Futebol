from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from datetime import datetime

DADO_SILVER = Dataset("postgres://silver_players")

# Definição da DAG
@dag(
    dag_id='futebol_gold_analytics',
    schedule=[DADO_SILVER],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['futebol', 'gold']
)
def futebol_gold():

    @task
    def transform_to_gold():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # 1. Tabela Individual com Rankings e Eficiência
        create_players_gold = """
            CREATE TABLE IF NOT EXISTS gold_players_ranking (
                player_id INT PRIMARY KEY,
                name VARCHAR(255),
                team VARCHAR(255),
                ga_total INTEGER,           -- G+A
                shot_efficiency FLOAT,      -- Gols/Chutes
                dribble_success_rate FLOAT, -- Dribles %
                creativity_idx FLOAT,       -- Passes Chave / 90min
                power_score FLOAT,          -- Pontuação de 0 a 100 consolidada
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        
        # 2. Tabela de Elenco (Power Ranking de Times)
        create_teams_gold = """
            CREATE TABLE IF NOT EXISTS gold_squad_power_ranking (
                team VARCHAR(255) PRIMARY KEY,
                avg_power_score FLOAT,      -- Média de força do elenco
                elite_players_count INTEGER, -- Jogadores com Power Score > 80
                total_ga INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        pg_hook.run([create_players_gold, create_teams_gold])

        # 3. Lógica do Ranking Individual (Power Score)
        # Cálculo que pesa G+A, Rating e Criatividade
        insert_players_ranking = """
            INSERT INTO gold_players_ranking (
                player_id, name, team, ga_total, shot_efficiency, 
                dribble_success_rate, creativity_idx, power_score
            )
            SELECT 
                player_id, name, team,
                (goals + assists) as ga_total,
                ROUND((goals::float / NULLIF(shots_total, 0) * 100)::numeric, 2) as shot_efficiency,
                ROUND((dribbles_success::float / NULLIF(dribbles_attempts, 0) * 100)::numeric, 2) as dribble_success_rate,
                ROUND((passes_key::float / NULLIF(minutes, 0) * 90)::numeric, 2) as creativity_idx,
                -- Cálculo do Power Score: Peso 40% Rating, 30% G+A, 30% Volume (Touches)
                ROUND(((rating * 10) * 0.4 + (goals + assists) * 2 * 0.3 + (touches / 100.0) * 0.3)::numeric, 2) as power_score
            FROM silver_players
            ON CONFLICT (player_id) DO UPDATE SET
                power_score = EXCLUDED.power_score,
                updated_at = CURRENT_TIMESTAMP;
        """

        # 4. Lógica do Ranking de Elenco
        insert_teams_ranking = """
            INSERT INTO gold_squad_power_ranking (team, avg_power_score, elite_players_count, total_ga)
            SELECT 
                team,
                ROUND(AVG(power_score)::numeric, 2) as avg_power_score,
                COUNT(*) FILTER (WHERE power_score >= 75) as elite_players_count,
                SUM(ga_total) as total_ga
            FROM gold_players_ranking
            GROUP BY team
            ON CONFLICT (team) DO UPDATE SET
                avg_power_score = EXCLUDED.avg_power_score,
                elite_players_count = EXCLUDED.elite_players_count,
                updated_at = CURRENT_TIMESTAMP;
        """
        pg_hook.run([insert_players_ranking, insert_teams_ranking])
        print("Camada Gold atualizada com sucesso!")

    # Executa a tarefa
    transform_to_gold()

# Instancia a DAG
futebol_gold()