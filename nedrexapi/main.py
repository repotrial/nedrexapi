import os

from fastapi import FastAPI, APIRouter, Request
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from nedrexapi.config import config, parse_config
from nedrexapi.db import MongoInstance, create_directories

parse_config(os.environ["NEDREX_CONFIG"])
MongoInstance.connect(config["api.mode"])
create_directories()

from nedrexapi.routers import admin as _admin
from nedrexapi.routers import bicon as _bicon
from nedrexapi.routers import closeness as _closeness
from nedrexapi.routers import comorbiditome as _comorbiditome
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
    redoc_url="/" if config.get("api.base") is None else config["api.base"],
)

if config["api.rate_limiting_enabled"]:
    from nedrexapi.common import limiter

    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)


def _get_prefix(base: str, prefix: str):
    if base:
        prefix = f"{base}{prefix}" if base != "/" else prefix
    return prefix[:-1] if prefix.endswith("/") else prefix


bases = ["/"]

if config.get("api.base") is not None:
    if config["api.base"] != "/":
        bases = [config["api.base"], "/"]

for app_base in bases:
    app.include_router(_general.router, prefix=_get_prefix(app_base, "/"), tags=["General"])
    app.include_router(_disorder.router, prefix=_get_prefix(app_base, "/disorder"), tags=["Disorder"])
    app.include_router(_ppi.router, prefix=_get_prefix(app_base, "/"), tags=["PPI routes"])
    app.include_router(_relations.router, prefix=_get_prefix(app_base, "/relations"), tags=["Relations"])
    app.include_router(_graph.router, prefix=_get_prefix(app_base, "/graph"), tags=["Graph"])
    app.include_router(_bicon.router, prefix=_get_prefix(app_base, "/bicon"), tags=["BiCoN"])
    app.include_router(_static.router, prefix=_get_prefix(app_base, "/static"), tags=["Static"])
    app.include_router(_must.router, prefix=_get_prefix(app_base, "/must"), tags=["MuST"])
    app.include_router(_diamond.router, prefix=_get_prefix(app_base, "/diamond"), tags=["DIAMOnD"])
    app.include_router(_domino.router, prefix=_get_prefix(app_base, "/domino"), tags=["DOMINO"])
    app.include_router(_robust.router, prefix=_get_prefix(app_base, "/robust"), tags=["ROBUST"])
    app.include_router(_kpm.router, prefix=_get_prefix(app_base, "/kpm"), tags=["KPM"])
    app.include_router(_trustrank.router, prefix=_get_prefix(app_base, "/trustrank"), tags=["TrustRank"])
    app.include_router(_closeness.router, prefix=_get_prefix(app_base, "/closeness"), tags=["Closeness"])
    app.include_router(_validation.router, prefix=_get_prefix(app_base, "/validation"), tags=["Validation"])
    app.include_router(_admin.router, prefix=_get_prefix(app_base, "/admin"), tags=["Admin"])
    app.include_router(_variant.router, prefix=_get_prefix(app_base, "/variants"), tags=["Variants"])
    app.include_router(_neo4j.router, prefix=_get_prefix(app_base, "/neo4j"), tags=["Neo4j"])
    app.include_router(_comorbiditome.router, prefix=_get_prefix(app_base, "/comorbiditome"),
                       tags=["Comorbiditome & ICD10 Mapping"])
