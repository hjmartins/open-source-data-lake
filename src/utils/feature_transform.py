import pandas as pd


def create_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria features temporais (lags, médias móveis e componentes de data).
    """
    df = df.copy()

    # Garantir que o index seja datetime
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    
    # LAGS temp_max
    
    for i in [1, 2, 3]:
        df[f"temp_lag_{i}"] = df["temp_max"].shift(i)
        df[f"precip_lag_{i}"] = df["precipitation_sum"].shift(i)

  
    # ROLLING MEAN
   
    df["temp_rolling_mean_7"] = (
        df["temp_max"]
        .shift(1)
        .rolling(window=7)
        .mean()
    )

    df["precip_rolling_mean_7"] = (
        df["precipitation_sum"]
        .shift(1)
        .rolling(window=7)
        .mean()
    )

    
    # COMPONENTES TEMPORAIS
    
    df["day_of_year"] = df.index.dayofyear
    df["month"] = df.index.month

    return df


def create_targets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria os targets (valores de amanhã).
    """
    df = df.copy()

    df["target_temp_tomorrow"] = df["temp_max"].shift(-1)
    df["target_precip_tomorrow"] = df["precipitation_sum"].shift(-1)

    return df


def build_training_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline completa de transformação para treino.
    Retorna dataframe pronto para salvar em parquet.
    """
    df = create_temporal_features(df)
    df = create_targets(df)

    
    df = df.dropna()

    return df