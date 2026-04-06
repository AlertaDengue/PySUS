__all__ = [
    "CNES",
    "PNI",
    "SIA",
    "SINAN",
]

from typing import List, Optional, Union

from pysus.utils import MONTHS, UFs, parse_UFs, to_list, zfill_year

from .models import Dataset, Resource


class CNES(Dataset):
    name = "CNES"
    ids = (
        "40a0d093-b12f-44a4-bdc7-bae8eb54dd04",
        "9455b341-b06e-408e-8e10-54b32b3d74ec",
    )

    def describe(self, file: Resource):
        ...

    def format(self, file: Resource) -> tuple:
        ...

    def get_files(
        self,
        year: Optional[Union[list, str, int]] = None,
        month: Optional[Union[list, str, int]] = None,
    ) -> List[Resource]:
        ...


class PNI(Dataset):
    name = "PNI"
    ids = (
        "2989d396-cb09-47e7-a3b8-a4b951ca0200",
        "543aa08a-46c4-44e8-802e-198daa30753d",
        "04292d08-ee4f-463a-b7b5-76cfb76775b3",
        "7ed6eecc-c254-475c-92c5-daba5727596b",
        "783b7456-6a6c-4025-a8bd-8e9caa0fb962",
        "c6c3c6f3-2026-48a2-84ac-d8039714a0ba",
        "9a25b796-80e3-444a-a4e7-405f5596d8ab",
    )

    def describe(self, file: Resource):
        ...

    def format(self, file: Resource) -> tuple:
        ...

    def get_files(
        self,
        year: Optional[Union[list, str, int]] = None,
        month: Optional[Union[list, str, int]] = None,
    ) -> List[Resource]:
        ...


class SIA(Dataset):
    name = "SIA"
    ids = ("9a335cb7-2b4f-4fce-8947-e8441b4a90af",)

    def describe(self, file: Resource):
        ...

    def format(self, file: Resource) -> tuple:
        ...

    def get_files(
        self,
        group: Union[List[str], str],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
        month: Optional[Union[list, str, int]] = None,
    ) -> List[Resource]:
        ...


class SINAN(Dataset):
    name = "SINAN"
    ids = (
        "4d5e5d44-58a8-4d67-b8aa-4ef1e4b00a1c",
        "5699abe0-0510-4da8-b47d-209b3bb32b34",
        "4557ba96-7d52-4a56-bd6f-f99a5af09f77",
        "740ce8f4-7a5d-4351-aad4-7623f2490ada",
    )

    def describe(self, file: Resource):
        ...

    def format(self, file: Resource) -> tuple:
        ...

    def get_files(
        self,
        dis_code: Optional[Union[str, list]] = None,
        year: Optional[Union[str, int, list]] = None,
    ) -> List[Resource]:
        ...
