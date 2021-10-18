import uvicorn as _uvicorn

from uvicorn.config import LOGGING_CONFIG as _LOGGING_CONFIG  # type: ignore

from nedrexapi.db import MongoInstance
from nedrexapi.config import parse_config, config


parse_config(".config.toml")
MongoInstance.connect("dev")

APP_STRING = "nedrexapi.main:app"


def run():
    _LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    _uvicorn.run(
        APP_STRING,
        port=config["api.port"],
        reload=True,
    )


if __name__ == "__main__":
    run()
