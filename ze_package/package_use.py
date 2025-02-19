import pandas as pd
from ze_package.ingestion import (
    ingest_from_pokedex_api, 
    connect_to_supabase, 
    dataframe_to_bucket,
    get_client_clickhouse,
    create_pokemon_table,
    fetch_pokemon_data,
    insert_pokemon_data
)
import os
from dotenv import load_dotenv

load_dotenv()

url = os.getenv('NEXT_PUBLIC_SUPABASE_URL')
key = os.getenv('NEXT_PUBLIC_SUPABASE_ANON_KEY')

ch_url = os.getenv('CLICKHOUSE_URL')
ch_user = os.getenv('CLICKHOUSE_USER')
ch_password = os.getenv('CLICKHOUSE_PASSWORD')
ch_db = os.getenv('CLICKHOUSE_DB')

client = get_client_clickhouse(ch_url, 8123, ch_user, ch_password, ch_db)

create_pokemon_table(client)

poke_data = fetch_pokemon_data(67)
print(poke_data)
insert_pokemon_data(client, poke_data)

try:
    supabase_client = connect_to_supabase(url, key)
    data = ingest_from_pokedex_api()
    df = pd.DataFrame(data['results'])
    result = dataframe_to_bucket(supabase_client, "meubocketkkk", df, "pokemons.csv")

except Exception as e:
    print(f"Erro durante a execução: {str(e)}")