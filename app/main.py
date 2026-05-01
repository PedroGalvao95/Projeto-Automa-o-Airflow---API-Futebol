import asyncio
import asyncpg
import csv

async def exportar():
    conn = await asyncpg.connect(
        host="localhost",
        port=5432,
        user="airflow",
        password="airflow",
        database="airflow"
    )

    # Exporta gold_players_ranking
    rows = await conn.fetch("SELECT * FROM gold_players_ranking")
    if rows:
        with open("gold_players.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(rows[0].keys())
            writer.writerows(rows)
        print(f"gold_players.csv exportado com {len(rows)} linhas!")
    else:
        print("Tabela gold_players_ranking está vazia!")

    # Exporta gold_squad_power_ranking
    rows2 = await conn.fetch("SELECT * FROM gold_squad_power_ranking")
    if rows2:
        with open("gold_squad.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(rows2[0].keys())
            writer.writerows(rows2)
        print(f"gold_squad.csv exportado com {len(rows2)} linhas!")
    else:
        print("Tabela gold_squad_power_ranking está vazia!")

    await conn.close()

asyncio.run(exportar())