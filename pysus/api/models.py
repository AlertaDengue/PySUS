import dateparser
from pydantic import BaseModel, ConfigDict, field_validator
from typing import Optional, Union
from datetime import datetime


class FileDescription(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    name: str
    group: str
    year: int
    size: int
    last_update: datetime
    uf: Optional[str] = None
    month: Optional[str] = None
    disease: Optional[str] = None

    @field_validator("last_update", mode="before")
    @classmethod
    def parse_modify_date(cls, v: Union[str, datetime]) -> datetime:
        if isinstance(v, datetime):
            return v

        parsed = dateparser.parse(str(v))
        if parsed:
            return parsed

        return datetime.now()
