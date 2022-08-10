from dataclasses import dataclass as _dataclass
from pprint import pformat as _pformat
from typing import Any as _Any
from typing import Mapping
from typing import Optional as _Optional

import toml as _toml  # type: ignore

from nedrexapi.exceptions import ConfigError as _ConfigError


@_dataclass
class _Config:
    data: _Optional[dict[_Any, _Any]] = None

    def __repr__(self) -> str:
        return _pformat(self.data)

    def from_file(self, infile: str) -> None:
        with open(infile, "r") as f:
            self.data = _toml.load(f)

    def __getitem__(self, path: str) -> _Any:
        if self.data is None:
            raise _ConfigError("config has not been parsed (currently None)")

        split_path = path.split(".")
        current: _Any = self.data

        for idx, val in enumerate(split_path):
            if isinstance(current, Mapping):
                current = current.get(val)
            else:
                failed_path = ".".join(split_path[: idx + 1])
                raise _ConfigError(f"{failed_path!r} is not in config")

            if current is None:
                failed_path = ".".join(split_path[: idx + 1])
                raise _ConfigError(f"{failed_path!r} is not in config")

        return current

    def get(self, path: str) -> _Any:
        try:
            return self[path]
        except _ConfigError:
            return None


config = _Config()


def parse_config(infile: str) -> None:
    global config
    config.from_file(infile)
