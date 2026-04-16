from typing import Any

from .models import Dataset


class CNES(Dataset):
    ids: list[str] = [
        "40a0d093-b12f-44a4-bdc7-bae8eb54dd04",
        "9455b341-b06e-408e-8e10-54b32b3d74ec",
    ]

    @property
    def name(self) -> str:
        return "CNES"

    @property
    def long_name(self) -> str:
        return "Cadastro Nacional de Estabelecimentos de Saúde"

    @property
    def description(self) -> str:
        return (
            "O Cadastro Nacional de Estabelecimentos de Saúde (CNES) é o "
            "sistema de informação oficial de cadastramento de informações "
            "de todos os estabelecimentos de saúde no país."
        )

    def formatter(self, filename: str) -> dict[str, Any]:
        raise NotImplementedError()


class PNI(Dataset):
    ids: list[str] = [
        "2989d396-cb09-47e7-a3b8-a4b951ca0200",
        "543aa08a-46c4-44e8-802e-198daa30753d",
        "04292d08-ee4f-463a-b7b5-76cfb76775b3",
        "7ed6eecc-c254-475c-92c5-daba5727596b",
        "783b7456-6a6c-4025-a8bd-8e9caa0fb962",
        "c6c3c6f3-2026-48a2-84ac-d8039714a0ba",
        "9a25b796-80e3-444a-a4e7-405f5596d8ab",
    ]

    @property
    def name(self) -> str:
        return "PNI"

    @property
    def long_name(self) -> str:
        return "Programa Nacional de Imunizações"

    @property
    def description(self) -> str:
        return "O PNI monitora a cobertura vacinal e doses aplicadas no Brasil."

    def formatter(self, filename: str) -> dict[str, Any]:
        raise NotImplementedError()


class SIA(Dataset):
    ids: list[str] = [
        "9a335cb7-2b4f-4fce-8947-e8441b4a90af",
    ]

    @property
    def name(self) -> str:
        return "SIA"

    @property
    def long_name(self) -> str:
        return "Sistema de Informações Ambulatoriais"

    @property
    def description(self) -> str:
        return """
            O SIA acompanha as ações de saúde produzidas no âmbito ambulatorial.
        """

    def formatter(self, filename: str) -> dict[str, Any]:
        raise NotImplementedError()


class SINAN(Dataset):
    ids: list[str] = [
        "4d5e5d44-58a8-4d67-b8aa-4ef1e4b00a1c",
        "5699abe0-0510-4da8-b47d-209b3bb32b34",
        "4557ba96-7d52-4a56-bd6f-f99a5af09f77",
        "740ce8f4-7a5d-4351-aad4-7623f2490ada",
    ]

    @property
    def name(self) -> str:
        return "SINAN"

    @property
    def long_name(self) -> str:
        return "Sistema de Informação de Agravos de Notificação"

    @property
    def description(self) -> str:
        return """
            O SINAN é alimentado pela notificação de doenças de notificação
            compulsória
            """

    def formatter(self, filename: str) -> dict[str, Any]:
        raise NotImplementedError()


class SIM(Dataset):
    ids: list[str] = [
        "5f121f4d-47c6-428e-8ec6-e8ec56417172",
    ]

    @property
    def name(self) -> str:
        return "SIM"

    @property
    def long_name(self) -> str:
        return "Sistema de Informação sobre Mortalidade"

    @property
    def description(self) -> str:
        return """
            O SIM coleta dados sobre óbitos no país para análise epidemiológica.
        """

    def formatter(self, filename: str) -> dict[str, Any]:
        raise NotImplementedError()


class SINASC(Dataset):
    ids: list[str] = [
        "441cc6bd-684a-4afd-a88b-ba4734c9e83e",
    ]

    @property
    def name(self) -> str:
        return "SINASC"

    @property
    def long_name(self) -> str:
        return "Sistema de Informações sobre Nascidos Vivos"

    @property
    def description(self) -> str:
        return """
            O SINASC fornece subsídios para o diagnóstico de saúde e
            planejamento de políticas de natalidade.
        """

    def formatter(self, filename: str) -> dict[str, Any]:
        raise NotImplementedError()


AVAILABLE_DATABASES: list[type[Dataset]] = [
    CNES,
    PNI,
    SIA,
    SIM,
    SINAN,
    SINASC,
]
