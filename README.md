# ⚽ Pipeline de Dados: Premier League Stats (Medallion Architecture)

Este projeto implementa um pipeline de dados ponta a ponta que automatiza a extração, o processamento e a análise de estatísticas de jogadores da **Premier League (Temporada 2024)**. A solução utiliza a **Arquitetura Medalhão** para organizar o fluxo de dados desde o estado bruto até a geração de rankings de performance[cite: 4, 6, 9].

##  Arquitetura do Projeto

O workflow é orquestrado pelo **Apache Airflow**, garantindo a integridade dos dados através de três camadas:

1.  **[Bronze (Raw Data)](dags/dag_futebol_bronze.py):** Extração via `API-Football` e armazenamento do JSON bruto em PostgreSQL (`bronze_players`).
2.  **[Silver (Structured Data)](dags/dag_futebol_silver.py):** Limpeza, tipagem e normalização dos dados brutos para um formato tabular estruturado (`silver_players`).
3.  **[Gold (Analytics Data)](dags/dag_futebol_gold.py):** Geração de tabelas de consumo com métricas avançadas, como o **Power Score** (ranking de força) e eficiência de chutes.

## 🛠️ Tecnologias Utilizadas

*   **Orquestração:** [Apache Airflow](https://airflow.apache.org/).
*   **Banco de Dados:** [PostgreSQL](https://www.postgresql.org/).
*   **Conteneirização:** [Docker](https://www.docker.com/) & Docker Compose.
*   **Linguagem:** Python (Requests / SQL).
*   **Fonte de Dados:** [API-Football (v3)](https://www.api-football.com/).

## 🚀 Como Executar

O projeto está configurado para rodar em containers, isolando todas as dependências.

### 1. Pré-requisitos
*   Docker e Docker Compose instalados.
*   Chave de acesso da API-Football.

### 2. Configuração de Ambiente
Crie um arquivo `.env` na raiz do projeto com o UID do Airflow (conforme definido no projeto):
```bash
AIRFLOW_UID=50000
```
No painel do Airflow (Admin -> Variables), cadastre a chave api_football_key com seu token da API.

### 3. Inicialização
```bash
# Subir os serviços (Airflow, Postgres, Redis)
docker-compose up -d
```
Acesse a interface em localhost:8080 (usuário: airflow | senha: airflow).

### Estrutura de Automação
As DAGs foram desenhadas para serem interdependentes via Airflow Datasets, disparando automaticamente conforme o dado é atualizado[cite: 4, 6, 9]:
*   futebol_bronze_extraction: Coleta dados de 20 times, com tratamento de rate limit.
*   futebol_silver_transformation: Processa a limpeza assim que a Bronze é finalizada.
*   futebol_gold_analytics: Atualiza os rankings de elite e eficiência dos elencos.

### DAG Grid
<img width="1903" height="861" alt="image" src="https://github.com/user-attachments/assets/4a10cacb-fab9-4feb-b6a3-90812ab937c6" />

### Grafo DAG Bronze
<img width="1898" height="868" alt="image" src="https://github.com/user-attachments/assets/fdd66ccc-a2cd-463f-9f89-6dc1889d398f" />

### Grafo DAG Silver
<img width="1899" height="869" alt="image" src="https://github.com/user-attachments/assets/332c03c5-6119-49fc-9d09-67126a6de7a2" />


### Grafo DAG Gold
<img width="1899" height="866" alt="image" src="https://github.com/user-attachments/assets/85742496-2e83-4b70-aeee-cff25a9f1ae6" />

### DBeaver com dados da camada Silver
<img width="1424" height="802" alt="WhatsApp Image 2026-04-30 at 22 15 48" src="https://github.com/user-attachments/assets/c6f273e2-f081-40da-861b-6ba4801cac7b" />

### Atualização futura
*  Inclusão de análise no PowerBI por conexão às tabelas Silver e Gold diretamente do banco de dados

### Desenvolvido por Pedro Galvão
