from typing import List, Optional

import requests
from pydantic import TypeAdapter
from pysus import __version__
from pysus.api.dadosgov.models import Dataset, DatasetSummary
from pysus.api.models import BaseRemoteClient


class DadosGov(BaseRemoteClient):
    def __init__(self, token: str):
        self.base_url = "https://dados.gov.br/dados/api"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": f"PySUS/{__version__}",
                "chave-api-dados-abertos": token,
            }
        )

    def _get(self, endpoint: str, params: Optional[dict] = None):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def list_datasets(
        self,
        pagina: int = 1,
        nome_conjunto: Optional[str] = None,
        dados_abertos: Optional[bool] = None,
        is_privado: bool = False,
        id_organizacao: Optional[str] = None,
    ) -> List[DatasetSummary]:
        params = {
            "pagina": pagina,
            "nomeConjuntoDados": nome_conjunto,
            "dadosAbertos": dados_abertos,
            "isPrivado": is_privado,
            "idOrganizacao": id_organizacao,
        }

        params = {k: v for k, v in params.items() if v is not None}

        data = self._get("/publico/conjuntos-dados", params=params)
        adapter = TypeAdapter(List[DatasetSummary])
        return adapter.validate_python(data)

    def get_dataset(self, id: str) -> Dataset:
        data = self._get(f"/publico/conjuntos-dados/{id}")
        return Dataset.model_validate(data)
