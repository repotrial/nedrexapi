from fastapi import FastAPI
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from nedrexapi.config import config, parse_config
from nedrexapi.db import MongoInstance, create_directories

parse_config(".config.toml")
MongoInstance.connect(config["api.mode"])
create_directories()

from nedrexapi.routers import admin as _admin
from nedrexapi.routers import bicon as _bicon
from nedrexapi.routers import closeness as _closeness
from nedrexapi.routers import diamond as _diamond
from nedrexapi.routers import disorder as _disorder
from nedrexapi.routers import domino as _domino
from nedrexapi.routers import general as _general
from nedrexapi.routers import graph as _graph
from nedrexapi.routers import kpm as _kpm
from nedrexapi.routers import must as _must
from nedrexapi.routers import neo4j as _neo4j
from nedrexapi.routers import ppi as _ppi
from nedrexapi.routers import relations as _relations
from nedrexapi.routers import robust as _robust
from nedrexapi.routers import static as _static
from nedrexapi.routers import trustrank as _trustrank
from nedrexapi.routers import validation as _validation
from nedrexapi.routers import variant as _variant

app = FastAPI(
    title="NeDRexAPI",
    description="""
An API for accessing the NeDRex database.
By using this API, you agree to the
[NeDRex platform licence](https://raw.githubusercontent.com/repotrial/nedrex_platform_licence/main/licence.txt).
You must not use this API if you do not or cannot agree to this licence.


For details about the edge and node types in the database, please consult this
[Google Doc](https://docs.google.com/document/d/1ji9_vZJa5XoLXQspKkb3eJ1fn4Mr7CPghCQRavmi1Ac/edit?usp=sharing)

For a tutorial on using the API, please consult
[this Google doc](https://docs.google.com/document/d/1_3juAFAYl2bXaJEsPwKTxazcv2TwtST-QM8PXj5c2II/edit?usp=sharing).
""",
    version="2.0.0a",
    docs_url=None,
    redoc_url="/",
)


if config["api.rate_limiting_enabled"]:
    from nedrexapi.common import limiter

    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)

app.include_router(_general.router, tags=["General"])
app.include_router(_disorder.router, prefix="/disorder", tags=["Disorder"])
app.include_router(_ppi.router, tags=["PPI routes"])
app.include_router(_relations.router, prefix="/relations", tags=["Relations"])
app.include_router(_graph.router, prefix="/graph", tags=["Graph"])
app.include_router(_bicon.router, prefix="/bicon", tags=["BiCoN"])
app.include_router(_static.router, prefix="/static", tags=["Static"])
app.include_router(_must.router, tags=["MuST"], prefix="/must")
app.include_router(_diamond.router, prefix="/diamond", tags=["DIAMOnD"])
app.include_router(_domino.router, prefix="/domino", tags=["DOMINO"])
app.include_router(_robust.router, prefix="/robust", tags=["ROBUST"])
app.include_router(_kpm.router, prefix="/kpm", tags=["KPM"])
app.include_router(_trustrank.router, prefix="/trustrank", tags=["TrustRank"])
app.include_router(_closeness.router, prefix="/closeness", tags=["Closeness"])
app.include_router(_validation.router, prefix="/validation", tags=["Validation"])
app.include_router(_admin.router, prefix="/admin", tags=["Admin"])
app.include_router(_variant.router, prefix="/variants", tags=["Variants"])
app.include_router(_neo4j.router, prefix="/neo4j", tags=["Neo4j"])
