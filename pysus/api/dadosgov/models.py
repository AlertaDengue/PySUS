import pathlib
import zipfile
from datetime import datetime as dt
from typing import Annotated, Any, List, Optional

import anyio
import httpx
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from pysus.api.models import BaseRemoteDataset, BaseRemoteFile


def to_datetime(value: Any) -> Optional[dt]:
    if not value or not isinstance(value, str) or "Indisponível" in value:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return dt.strptime(value, fmt)
        except ValueError:
            continue
    return None


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("sim", "true", "1")


DateTime = Annotated[Optional[dt], BeforeValidator(to_datetime)]
Bool = Annotated[bool, BeforeValidator(to_bool)]


class Tag(BaseModel):
    id: str
    name: str
    display_name: Optional[str] = None


class Resource(BaseRemoteFile):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    title: str = Field(alias="titulo")
    url: str = Field(alias="link")
    api_size: int = Field(alias="tamanho")
    last_modified: DateTime = Field(None, alias="dataUltimaAtualizacaoArquivo")
    file_name: Optional[str] = Field(None, alias="nomeArquivo")

    def __init__(self, **data):
        url = data.get("link") or data.get("url")
        basename = url.split("/")[-1].rstrip(".zip").replace("_csv", ".csv")
        super().__init__(
            basename=basename,
            path=url,
            extension=pathlib.Path(basename).suffix,
            type="RemoteResource",
            **data,
        )

    async def _download(self, output: pathlib.Path) -> pathlib.Path:
        tmp_file = output.with_suffix(".download")

        async with httpx.AsyncClient(verify=False) as client:
            async with client.stream(
                "GET", self.url, follow_redirects=True
            ) as response:
                response.raise_for_status()
                with open(tmp_file, "wb") as f:
                    async for chunk in response.aiter_bytes():
                        f.write(chunk)

        if zipfile.is_zipfile(tmp_file):

            def _extract():
                with zipfile.ZipFile(tmp_file) as z:
                    members = z.namelist()
                    z.extractall(output.parent)
                    return (
                        output.parent / members[0]
                        if len(members) == 1
                        else output.parent
                    )

            final_path = await anyio.to_thread.run_sync(_extract)
            tmp_file.unlink()
            return final_path

        tmp_file.rename(output)
        return output


class Dataset(BaseRemoteDataset):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    title: str = Field(alias="titulo")
    slug: str = Field(alias="nome")
    resources: List[Resource] = Field(default_factory=list, alias="recursos")
    file_updated: DateTime = Field(None, alias="dataUltimaAtualizacaoArquivo")

    async def get_files(self, **kwargs) -> List[Resource]:
        for res in self.resources:
            res.description = self.describe(res)
        return self.resources

    def describe(self, resource: Resource):
        return FileDescription(
            name=resource.basename,
            group=self.slug,
            year=0,
            size=resource.api_size,
            last_update=resource.last_modified
            or self.file_updated
            or dt.now(),
            uf=None,
            month=None,
            disease=self.title,
        )
