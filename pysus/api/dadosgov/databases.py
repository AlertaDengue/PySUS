"""Pre-configured health database definitions accessible via dados.gov.br."""

import re
from typing import Any

from pysus.utils import zfill_year

from .models import Dataset

MONTHS: dict[str, int] = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}


def _parse_year(val: str) -> int | None:
    """Parse a year string into an integer within the valid range."""
    try:
        y = int(val)
        return y if 1970 <= y <= 2100 else None
    except ValueError:
        return None


def _skip(name: str) -> bool:
    """Check whether a filename should be skipped by naming conventions."""
    return name.startswith("get_") or name.lower().endswith(".pdf")


class CNES(Dataset):
    """Cadastro Nacional de Estabelecimentos de Saúde (CNES)."""

    ids: list[str] = [
        "40a0d093-b12f-44a4-bdc7-bae8eb54dd04",
        "9455b341-b06e-408e-8e10-54b32b3d74ec",
    ]

    @property
    def name(self) -> str:
        """Return the short name.

        Returns
        -------
        str
            The abbreviated dataset name ``"CNES"``.
        """
        return "CNES"

    @property
    def long_name(self) -> str:
        """Return the human-readable name.

        Returns
        -------
        str
            The full Portuguese name of the dataset.
        """
        return "Cadastro Nacional de Estabelecimentos de Saúde"

    @property
    def description(self) -> str:
        """Return a description of the dataset.

        Returns
        -------
        str
            A Portuguese description of the CNES information system.
        """
        return (
            "O Cadastro Nacional de Estabelecimentos de Saúde (CNES) é o "
            "sistema de informação oficial de cadastramento de informações "
            "de todos os estabelecimentos de saúde no país."
        )

    def formatter(self, filename: str) -> dict[str, Any]:
        """Parse a CNES filename and extract metadata.

        Parameters
        ----------
        filename : str
            The name of the file to parse.

        Returns
        -------
        dict[str, Any]
            A dictionary with keys ``state``, ``year``, and ``month``.
            Unrecognised files return ``None`` for all keys.
        """
        try:
            name = filename.strip()
            if _skip(name):
                return {"state": None, "year": None, "month": None}

            m = re.search(r"_(\d{2})-(\d{4})\.csv$", name)
            if m:
                return {
                    "state": None,
                    "year": _parse_year(m.group(2)),
                    "month": int(m.group(1)),
                }

            return {"state": None, "year": None, "month": None}
        except (IndexError, ValueError):
            return {"state": None, "year": None, "month": None}


class PNI(Dataset):
    """Programa Nacional de Imunizações (PNI)."""

    ids: list[str] = [
        "2989d396-cb09-47e7-a3b8-a4b951ca0200",
        "543aa08a-46c4-44e8-802e-198daa30753d",
        "04292d08-ee4f-463a-b7b5-76cfb76775b3",
        "7ed6eecc-c254-475c-92c5-daba5727596b",
        "783b7456-6a6c-4025-a8bd-8e9caa0fb962",
        "c6c3c6f3-2026-48a2-84ac-d8039714a0ba",
        "9a25b796-80e3-444a-a4e7-405f5596d8ab",
    ]

    _PNI_PREFIX = "doses-aplicadas-pelo-programa-de-nacional-de-imunizacoes-pni"

    group_aliases: dict[str, str] = {
        _PNI_PREFIX: "DPNI",
        f"{_PNI_PREFIX}-2020": "DPNI",
        f"{_PNI_PREFIX}-2021": "DPNI",
        f"dataset-{_PNI_PREFIX}_2022": "DPNI",
        f"{_PNI_PREFIX}-2023": "DPNI",
        f"{_PNI_PREFIX}-2025": "DPNI",
        f"{_PNI_PREFIX}-2026": "DPNI",
    }

    @property
    def name(self) -> str:
        """Return the short name.

        Returns
        -------
        str
            The abbreviated dataset name ``"PNI"``.
        """
        return "PNI"

    @property
    def long_name(self) -> str:
        """Return the human-readable name.

        Returns
        -------
        str
            The full Portuguese name of the dataset.
        """
        return "Programa Nacional de Imunizações"

    @property
    def description(self) -> str:
        """Return a description of the dataset.

        Returns
        -------
        str
            A Portuguese description of the PNI vaccination monitoring system.
        """
        return "O PNI monitora a cobertura vacinal e doses aplicadas no Brasil."

    def formatter(self, filename: str) -> dict[str, Any]:
        """Parse a PNI vaccination filename into month and year.

        Parameters
        ----------
        filename : str
            The name of the file to parse.

        Returns
        -------
        dict[str, Any]
            A dictionary with keys ``state``, ``year``, and ``month``.
            Unrecognised files return ``None`` for all keys.
        """
        try:
            name = filename.strip().lower()
            if _skip(name):
                return {"state": None, "year": None, "month": None}

            m = re.match(r"vacinacao_(\w{3})_(\d{4})_csv\.zip", name)
            if m:
                month = MONTHS.get(m.group(1))
                year = _parse_year(m.group(2))
                return {"state": None, "year": year, "month": month}

            return {"state": None, "year": None, "month": None}
        except (IndexError, ValueError):
            return {"state": None, "year": None, "month": None}


class SIA(Dataset):
    """Sistema de Informações Ambulatoriais (SIA)."""

    ids: list[str] = [
        "9a335cb7-2b4f-4fce-8947-e8441b4a90af",
    ]

    @property
    def name(self) -> str:
        """Return the short name.

        Returns
        -------
        str
            The abbreviated dataset name ``"SIA"``.
        """
        return "SIA"

    @property
    def long_name(self) -> str:
        """Return the human-readable name.

        Returns
        -------
        str
            The full Portuguese name of the dataset.
        """
        return "Sistema de Informações Ambulatoriais"

    @property
    def description(self) -> str:
        """Return a description of the dataset.

        Returns
        -------
        str
            A Portuguese description of the SIA outpatient information system.
        """
        return """
            O SIA acompanha as ações de saúde produzidas no âmbito ambulatorial.
        """

    def formatter(self, filename: str) -> dict[str, Any]:
        """Parse an SIA filename into year.

        Parameters
        ----------
        filename : str
            The name of the file to parse.

        Returns
        -------
        dict[str, Any]
            A dictionary with keys ``state``, ``year``, and ``month``.
            Unrecognised files return ``None`` for all keys.
        """
        try:
            name = filename.strip().lower()
            if _skip(name):
                return {"state": None, "year": None, "month": None}

            m = re.search(r"_(\d{4})_\.csv$", name)
            if m:
                return {
                    "state": None,
                    "year": _parse_year(m.group(1)),
                    "month": None,
                }

            m = re.search(r"_(\w{3})-out_(\d{4})_\.csv$", name)
            if m:  # pragma: no cover
                return {
                    "state": None,
                    "year": _parse_year(m.group(2)),
                    "month": None,
                }

            return {"state": None, "year": None, "month": None}
        except (IndexError, ValueError):
            return {"state": None, "year": None, "month": None}


class SINAN(Dataset):
    """Sistema de Informação de Agravos de Notificação (SINAN)."""

    ids: list[str] = [
        "4d5e5d44-58a8-4d67-b8aa-4ef1e4b00a1c",
        "5699abe0-0510-4da8-b47d-209b3bb32b34",
        "4557ba96-7d52-4a56-bd6f-f99a5af09f77",
        "740ce8f4-7a5d-4351-aad4-7623f2490ada",
        "cf044c1b-b966-4d0e-bab0-f3aa65897b7d",
        "2d4997fb-cd11-4ce2-b217-09cd50e3151f",
        "8a585222-4c2e-43b7-807d-59355ee79c48",
        "527e8665-de64-4f81-b7c3-40b59c7d1d3c",
    ]

    group_aliases: dict[str, str] = {
        "arboviroses-dengue": "DENG",
        "arboviroses-febre-de-chikungunya": "CHIK",
        "arboviroses-zika-virus": "ZIKA",
        "hanseniase": "HANS",
        "dados-tuberculose": "TUBE",
        "sifilis": "SIFA",
    }

    @property
    def name(self) -> str:
        """Return the short name.

        Returns
        -------
        str
            The abbreviated dataset name ``"SINAN"``.
        """
        return "SINAN"

    @property
    def long_name(self) -> str:
        """Return the human-readable name.

        Returns
        -------
        str
            The full Portuguese name of the dataset.
        """
        return "Sistema de Informação de Agravos de Notificação"

    @property
    def description(self) -> str:
        """Return a description of the dataset.

        Returns
        -------
        str
            A Portuguese description of the SINAN notifiable diseases system.
        """
        return """
            O SINAN é alimentado pela notificação de doenças de notificação
            compulsória
            """

    def formatter(self, filename: str) -> dict[str, Any]:
        """Parse a SINAN filename into state and year.

        Parameters
        ----------
        filename : str
            The name of the file to parse.

        Returns
        -------
        dict[str, Any]
            A dictionary with keys ``state``, ``year``, and ``month``.
            Unrecognised files return ``None`` for all keys.
        """
        try:
            name = filename.strip().upper()
            if _skip(name):
                return {"state": None, "year": None, "month": None}

            m = re.match(r"(\w{4})(BR)(\d{2})\.CSV\.ZIP", name)
            if m:
                return {
                    "state": m.group(2),
                    "year": zfill_year(m.group(3)),
                    "month": None,
                }

            m = re.match(r"MPX_(\d{4})_OPENDATASUS\.CSV\.ZIP", name)
            if m:
                return {
                    "state": None,
                    "year": _parse_year(m.group(1)),
                    "month": None,
                }

            return {"state": None, "year": None, "month": None}
        except (IndexError, ValueError):
            return {"state": None, "year": None, "month": None}


class SIM(Dataset):
    """Sistema de Informação sobre Mortalidade (SIM)."""

    ids: list[str] = [
        "5f121f4d-47c6-428e-8ec6-e8ec56417172",
    ]

    group_aliases: dict[str, str] = {
        "sim-1979-2019": "DO",
    }

    @property
    def name(self) -> str:
        """Return the short name.

        Returns
        -------
        str
            The abbreviated dataset name ``"SIM"``.
        """
        return "SIM"

    @property
    def long_name(self) -> str:
        """Return the human-readable name.

        Returns
        -------
        str
            The full Portuguese name of the dataset.
        """
        return "Sistema de Informação sobre Mortalidade"

    @property
    def description(self) -> str:
        """Return a description of the dataset.

        Returns
        -------
        str
            A Portuguese description of the SIM mortality information system.
        """
        return """
            O SIM coleta dados sobre óbitos no país para análise epidemiológica.
        """

    def formatter(self, filename: str) -> dict[str, Any]:
        """Parse a SIM filename into year.

        Parameters
        ----------
        filename : str
            The name of the file to parse.

        Returns
        -------
        dict[str, Any]
            A dictionary with keys ``state``, ``year``, and ``month``.
            Unrecognised files return ``None`` for all keys.
        """
        try:
            name = filename.strip()
            if _skip(name):
                return {"state": None, "year": None, "month": None}

            m = re.search(r"Mortalidade_Geral_(\d{4})_csv\.zip", name)
            if m:
                return {
                    "state": None,
                    "year": _parse_year(m.group(1)),
                    "month": None,
                }

            m = re.match(r"DO(\d{2})OPEN", name)
            if m:
                return {
                    "state": None,
                    "year": zfill_year(m.group(1)),
                    "month": None,
                }

            return {"state": None, "year": None, "month": None}
        except (IndexError, ValueError):
            return {"state": None, "year": None, "month": None}


class SINASC(Dataset):
    """Sistema de Informações sobre Nascidos Vivos (SINASC)."""

    ids: list[str] = [
        "441cc6bd-684a-4afd-a88b-ba4734c9e83e",
    ]

    group_aliases: dict[str, str] = {
        "sistema-de-informacao-sobre-nascidos-vivos-sinasc-1996-a-20201": "DN",
    }

    @property
    def name(self) -> str:
        """Return the short name.

        Returns
        -------
        str
            The abbreviated dataset name ``"SINASC"``.
        """
        return "SINASC"

    @property
    def long_name(self) -> str:
        """Return the human-readable name.

        Returns
        -------
        str
            The full Portuguese name of the dataset.
        """
        return "Sistema de Informações sobre Nascidos Vivos"

    @property
    def description(self) -> str:
        """Return a description of the dataset.

        Returns
        -------
        str
            Portuguese description of the SINASC live birth system.
        """
        return """
            O SINASC fornece subsídios para o diagnóstico de saúde e
            planejamento de políticas de natalidade.
        """

    def formatter(self, filename: str) -> dict[str, Any]:
        """Parse a SINASC filename into year.

        Parameters
        ----------
        filename : str
            The name of the file to parse.

        Returns
        -------
        dict[str, Any]
            A dictionary with keys ``state``, ``year``, and ``month``.
            Unrecognised files return ``None`` for all keys.
        """
        try:
            name = filename.strip()
            if _skip(name):
                return {"state": None, "year": None, "month": None}

            m = re.search(r"SINASC_(\d{4})_csv\.zip", name)
            if m:
                return {
                    "state": None,
                    "year": _parse_year(m.group(1)),
                    "month": None,
                }

            m = re.search(r"DNBR(\d{4})_csv\.zip", name)
            if m:
                return {
                    "state": "BR",
                    "year": _parse_year(m.group(1)),
                    "month": None,
                }

            return {"state": None, "year": None, "month": None}
        except (IndexError, ValueError):
            return {"state": None, "year": None, "month": None}


class COVID19(Dataset):
    """Casos Confirmados de COVID-19."""

    ids: list[str] = [
        "1ba1801e-aec0-4dba-ae2a-7732f0a0c9f7",
    ]

    @property
    def name(self) -> str:
        """Return the short name.

        Returns
        -------
        str
            The abbreviated dataset name ``"COVID19"``.
        """
        return "COVID19"

    @property
    def long_name(self) -> str:
        """Return the human-readable name.

        Returns
        -------
        str
            The full Portuguese name of the dataset.
        """
        return "Casos Confirmados de COVID-19"

    @property
    def description(self) -> str:
        """Return a description of the dataset.

        Returns
        -------
        str
            A Portuguese description of the COVID-19 confirmed cases dataset.
        """
        return "Dados anonimizados de casos confirmados de COVID-19."

    def formatter(self, filename: str) -> dict[str, Any]:
        """Parse a COVID-19 filename and extract metadata.

        Parameters
        ----------
        filename : str
            The name of the file to parse.

        Returns
        -------
        dict[str, Any]
            A dictionary with keys ``state``, ``year``, and ``month``.
            Unrecognised files return ``None`` for all keys.
        """
        try:
            name = filename.strip().lower()
            if _skip(name) or name.endswith(".xlsx"):
                return {"state": None, "year": None, "month": None}

            if name.endswith(".csv"):
                return {"state": None, "year": None, "month": None}

            return {"state": None, "year": None, "month": None}
        except (IndexError, ValueError):
            return {"state": None, "year": None, "month": None}


AVAILABLE_DATABASES: list[type[Dataset]] = [
    CNES,
    PNI,
    SIA,
    SIM,
    SINAN,
    SINASC,
    COVID19,
]
