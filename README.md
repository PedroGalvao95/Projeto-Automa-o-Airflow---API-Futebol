# ⚽ Pipeline de Dados: Premier League Stats (Medallion Architecture)

Este projeto implementa um pipeline de dados ponta a ponta que automatiza a extração, o processamento e a análise de estatísticas de jogadores da **Premier League (Temporada 2024)**. A solução utiliza a **Arquitetura Medalhão** para organizar o fluxo de dados desde o estado bruto até a geração de rankings de performance[cite: 4, 6, 9].

##  Arquitetura do Projeto

O workflow é orquestrado pelo **Apache Airflow**, garantindo a integridade dos dados através de três camadas[cite: 3, 4, 6, 9]:

1.  **Bronze (Raw Data):** Extração via `API-Football` e armazenamento do JSON bruto em PostgreSQL (`bronze_players`)[cite: 4].
2.  **Silver (Structured Data):** Limpeza, tipagem e normalização dos dados brutos para um formato tabular estruturado (`silver_players`)[cite: 6].
3.  **Gold (Analytics Data):** Geração de tabelas de consumo com métricas avançadas, como o **Power Score** (ranking de força) e eficiência de chutes[cite: 9].

## 🛠️ Tecnologias Utilizadas

*   **Orquestração:** [Apache Airflow](https://airflow.apache.org/)[cite: 3].
*   **Banco de Dados:** [PostgreSQL](https://www.postgresql.org/)[cite: 3, 5].
*   **Conteneirização:** [Docker](https://www.docker.com/) & Docker Compose[cite: 3].
*   **Linguagem:** Python (Requests / SQL)[cite: 4, 6].
*   **Fonte de Dados:** [API-Football (v3)](https://www.api-football.com/)[cite: 4].

## 🚀 Como Executar

O projeto está configurado para rodar em containers, isolando todas as dependências[cite: 3].

### 1. Pré-requisitos
*   Docker e Docker Compose instalados[cite: 3].
*   Chave de acesso da API-Football[cite: 4].

### 2. Configuração de Ambiente
Crie um arquivo `.env` na raiz do projeto com o UID do Airflow (conforme definido no projeto)[cite: 1]:
```bash
AIRFLOW_UID=50000
```
No painel do Airflow (Admin -> Variables), cadastre a chave api_football_key com seu token da API[cite: 4].

### 3. Inicialização
```bash
# Subir os serviços (Airflow, Postgres, Redis)
docker-compose up -d
```
Acesse a interface em localhost:8080 (usuário: airflow | senha: airflow)[cite: 3].

### Estrutura de Automação
As DAGs foram desenhadas para serem interdependentes via Airflow Datasets, disparando automaticamente conforme o dado é atualizado[cite: 4, 6, 9]:
*   futebol_bronze_extraction: Coleta dados de 20 times, com tratamento de rate limit[cite: 4].
*   futebol_silver_transformation: Processa a limpeza assim que a Bronze é finalizada[cite: 6].
*   futebol_gold_analytics: Atualiza os rankings de elite e eficiência dos elencos[cite: 9].

### DAG Grid
<img width="1903" height="861" alt="image" src="https://github.com/user-attachments/assets/4a10cacb-fab9-4feb-b6a3-90812ab937c6" />

### Grafo DAG Bronze
<img width="1898" height="868" alt="image" src="https://github.com/user-attachments/assets/fdd66ccc-a2cd-463f-9f89-6dc1889d398f" />

### Grafo DAG Silver
<img width="1899" height="869" alt="image" src="https://github.com/user-attachments/assets/332c03c5-6119-49fc-9d09-67126a6de7a2" />


### Grafo DAG Gold
<img width="1899" height="866" alt="image" src="https://github.com/user-attachments/assets/85742496-2e83-4b70-aeee-cff25a9f1ae6" />

### Atualização futura
*  Inclusão de análise no PowerBI

### Desenvolvido por Pedro Galvão
