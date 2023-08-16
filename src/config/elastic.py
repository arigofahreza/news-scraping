from pydantic.v1 import BaseSettings
from elasticsearch import Elasticsearch


class ElasticConfig(BaseSettings):
    ELASTICSEARCH_HOST: str
    ELASTICSEARCH_PORT: str
    ELASTICSEARCH_TIMEOUT: int
    ELASTICSEARCH_INDEX: str

    class Config:
        env_file = ".env"


def elastic_client() -> Elasticsearch:
    elastic_config = ElasticConfig()
    return Elasticsearch(hosts=f'{elastic_config.ELASTICSEARCH_HOST}:{elastic_config.ELASTICSEARCH_PORT}',
                         timeout=elastic_config.ELASTICSEARCH_TIMEOUT)
