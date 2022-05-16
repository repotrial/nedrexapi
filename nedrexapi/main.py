from fastapi import FastAPI

from nedrexapi.db import MongoInstance, create_directories
from nedrexapi.config import parse_config

parse_config(".config.toml")
MongoInstance.connect()
create_directories()

from nedrexapi.routers import (  # noqa: E402
    bicon as _bicon,
    general as _general,
    disorder as _disorder,
    ppi as _ppi,
    relations as _relations,
    graph as _graph,
    static as _static,
    trustrank as _trustrank,
    diamond as _diamond,
    must as _must,
    closeness as _closeness,
    validation as _validation,
    admin as _admin,
    variant as _variant,
    neo4j as _neo4j,
)


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

app.include_router(_must.router, tags=["MuST"], prefix="/must")
app.include_router(_general.router, tags=["General"])
app.include_router(_disorder.router, prefix="/disorder", tags=["Disorder"])
app.include_router(_ppi.router, tags=["PPI routes"])
app.include_router(_relations.router, prefix="/relations", tags=["Relations"])
app.include_router(_graph.router, prefix="/graph", tags=["Graph"])
app.include_router(_bicon.router, prefix="/bicon", tags=["BiCoN"])
app.include_router(_static.router, prefix="/static", tags=["Static"])
app.include_router(_diamond.router, prefix="/diamond", tags=["DIAMOnD"])
app.include_router(_trustrank.router, prefix="/trustrank", tags=["TrustRank"])
app.include_router(_closeness.router, prefix="/closeness", tags=["Closeness"])
app.include_router(_validation.router, prefix="/validation", tags=["Validation"])
app.include_router(_admin.router, prefix="/admin", tags=["Admin"])
app.include_router(_variant.router, prefix="/variants", tags=["Variants"])
app.include_router(_neo4j.router, prefix="/neo4j", tags=["Neo4j"])
