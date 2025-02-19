import pytest
import clickhouse_connect
from unittest.mock import patch, MagicMock
from ze_package.ingestion import (
    connect_to_supabase,
    upload_to_bucket,
    ingest_from_pokedex_api,
    dataframe_to_bucket,
    get_client_clickhouse,
    create_pokemon_table,
    fetch_pokemon_data,
    insert_pokemon_data
)

# Testando conexão com Supabase
@patch("ze_package.ingestion.create_client")
def test_connect_to_supabase(mock_create_client):
    mock_create_client.return_value = MagicMock()
    client = connect_to_supabase("fake_url", "fake_key")
    assert client is not None

# Testando upload para Supabase
@patch("ze_package.ingestion.create_client")
def test_upload_to_bucket(mock_create_client):
    mock_supabase = mock_create_client()
    mock_supabase.storage.from_.return_value.upload.return_value = {"status": "success"}
    
    result = upload_to_bucket(mock_supabase, "test_bucket", "test.csv", b"data")
    assert result == {"status": "success"}

# Testando ingestão de API do Pokémon
@patch("ze_package.ingestion.requests.get")
def test_ingest_from_pokedex_api(mock_requests_get):
    mock_requests_get.return_value.status_code = 200
    mock_requests_get.return_value.json.return_value = {"results": []}
    
    result = ingest_from_pokedex_api()
    assert "results" in result

# Testando conexão com ClickHouse
@patch("ze_package.ingestion.clickhouse_connect.get_client")
def test_get_client_clickhouse(mock_get_client):
    mock_get_client.return_value = MagicMock()
    client = get_client_clickhouse("localhost", 8123, "user", "pass", "db")
    assert client is not None

# Testando criação da tabela Pokémon
@patch("ze_package.ingestion.clickhouse_connect.get_client")
def test_create_pokemon_table(mock_get_client):
    mock_client = mock_get_client()
    mock_client.command.return_value = None
    
    create_pokemon_table(mock_client)
    mock_client.command.assert_called_once()

# Testando busca de dados da PokéAPI
@patch("ze_package.ingestion.requests.get")
def test_fetch_pokemon_data(mock_requests_get):
    mock_requests_get.return_value.status_code = 200
    mock_requests_get.return_value.json.return_value = {
        "name": "pikachu",
        "types": [{"type": {"name": "electric"}}],
        "stats": [{"base_stat": 35}, {"base_stat": 55}, {"base_stat": 40}]
    }
    
    result = fetch_pokemon_data(25)
    assert result == (25, "Pikachu", "electric", 35, 55, 40)

# Testando inserção de Pokémon no ClickHouse
@patch("ze_package.ingestion.clickhouse_connect.get_client")
def test_insert_pokemon_data(mock_get_client):
    mock_client = mock_get_client()
    mock_client.insert.return_value = None
    
    data = (25, "Pikachu", "electric", 35, 55, 40)
    insert_pokemon_data(mock_client, data)
    mock_client.insert.assert_called_once()
