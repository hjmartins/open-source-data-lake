import pandas as pd
from sqlalchemy import create_engine

def get_weather_data(db_url: str, query: str) -> pd.DataFrame:
    """
    Conecta ao banco de dados, executa a query e retorna um DataFrame
    pronto para séries temporais (com a data como índice).
    """
    engine = create_engine(db_url)
    df = pd.read_sql(query, engine)
    
    # Garante que a coluna de data seja datetime e define como índice
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    return df