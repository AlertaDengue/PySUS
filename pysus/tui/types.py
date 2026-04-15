from typing import Protocol


class PySUSApp(Protocol):
    lang: str

    def populate_local_tree(self) -> None: ...  # noqa
    def notify(self, message: str, severity: str = "info") -> None: ...  # noqa
    def push_screen(self, screen, callback=None): ...  # noqa
    def pop_screen(self): ...  # noqa
    def switch_screen(self, name: str): ...  # noqa

    class _pysus:
        async def datasets(self): ...  # noqa
        def get_completed_remote_paths(self): ...  # noqa
        @property
        async def get_ducklake(self): ...  # noqa
        @property
        async def get_ftp(self): ...  # noqa
        @property
        async def get_dadosgov(self): ...  # noqa

    @property
    def pysus(self) -> _pysus: ...  # noqa
