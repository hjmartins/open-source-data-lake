import logging
import time

import requests

logger = logging.getLogger(__name__)


def safe_request(url: str, params: dict, retries: int = 5, backoff: float = 2.0) -> dict:
    """
    Faz uma requisição GET com retry exponencial.
    
    Args:
        url: URL do endpoint.
        params: Parâmetros da query string.
        retries: Número máximo de tentativas.
        backoff: Base do backoff exponencial (segundos).
    
    Returns:
        JSON da resposta como dict.
    
    Raises:
        requests.exceptions.RequestException: Após esgotar todas as tentativas.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.warning(f"Tentativa {attempt + 1}/{retries} falhou: {e}")
            if attempt < retries - 1:
                sleep_time = backoff ** attempt
                logger.info(f"Aguardando {sleep_time:.1f}s antes de tentar novamente...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Todas as {retries} tentativas esgotadas para {url}")
                raise
