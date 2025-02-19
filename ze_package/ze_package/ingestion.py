import pandas as pd
import requests
from supabase import create_client, Client
from io import BytesIO
import clickhouse_connect


def connect_to_supabase(url: str, key: str):
    try:
        client = create_client(url, key)
        return client
    except Exception as e:
        raise Exception(f"Erro ao conectar com Supabase: {str(e)}")

def upload_to_bucket(supabase, bucket_name: str, file_name: str, file):
    try:
        result = supabase.storage.from_(bucket_name).upload(file_name, file)
        return result
    except Exception as e:
        print(f"Erro ao fazer upload: {str(e)}")
        print(f"Bucket: {bucket_name}")
        print(f"Arquivo: {file_name}")
        raise

def ingest_from_pokedex_api():
    url = 'https://pokeapi.co/api/v2/pokemon?limit=10'
    response = requests.get(url)
    return response.json()

def dataframe_to_bucket(supabase, bucket_name: str, df: pd.DataFrame, file_name: str):
    try:
        buffer = BytesIO()
        df.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)
        content = buffer.getvalue()
        print(f"Tamanho dos dados: {len(content)} bytes")
        return upload_to_bucket(supabase, bucket_name, file_name, content)
    except Exception as e:
        print(f"Erro ao processar DataFrame: {str(e)}")
        raise

def get_client_clickhouse(host, port, user, password, database):
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database
    )

def create_pokemon_table(client):
    client.command("""
        CREATE TABLE IF NOT EXISTS pokemon (
            id UInt32,
            name String,
            type String,
            hp UInt8,
            attack UInt8,
            defense UInt8
        ) ENGINE = MergeTree()
        ORDER BY id
    """)

def fetch_pokemon_data(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}/"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        pokemon_name = data["name"].capitalize()
        pokemon_type = ", ".join([t["type"]["name"] for t in data["types"]])  # Pega os tipos
        hp = data["stats"][0]["base_stat"]  # HP
        attack = data["stats"][1]["base_stat"]  # Ataque
        defense = data["stats"][2]["base_stat"]  # Defesa

        return (pokemon_id, pokemon_name, pokemon_type, hp, attack, defense)
    else:
        print(f"Erro ao buscar Pokémon {pokemon_id}")
        return None
    
def insert_pokemon_data(client, data):
    if data:
        try:
            client.insert('pokemon', [data], column_names=['id', 'name', 'type', 'hp', 'attack', 'defense'])
            print(f"Pokémon {data[1]} inserido com sucesso!")
        except Exception as e:
            print(f"Erro ao inserir Pokémon: {str(e)}")
            raise
    else:
        print("Dados inválidos")