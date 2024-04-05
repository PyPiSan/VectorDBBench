from pydantic import BaseModel, SecretStr

from ..api import DBConfig, DBCaseConfig, MetricType


class VespaConfig(DBConfig):
    url: SecretStr
    api_key: SecretStr

    def to_dict(self) -> dict:
        return {
            "url": self.url.get_secret_value(),
            "auth_client_secret": self.api_key.get_secret_value(),
        }


class VespaIndexConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType | None = None

    def parse_metric(self) -> str:
        if self.metric_type == MetricType.L2:
            return "Euclid"

        if self.metric_type == MetricType.IP:
            return "Dot"

        return "Cosine"

    def index_param(self) -> dict:
        params = {"distance": self.parse_metric()}
        return params

    def search_param(self) -> dict:
        return {}
