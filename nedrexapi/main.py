from fastapi import FastAPI

from nedrexapi.routers import general as _general, disorder as _disorder, ppi as _ppi, relations as _relations

app = FastAPI(
    title="NeDRexAPI",
    description="""
An API for accessing the NeDRex database.
For details about the edge and node types in the database, please consult this
[Google Doc](https://docs.google.com/document/d/1ji9_vZJa5XoLXQspKkb3eJ1fn4Mr7CPghCQRavmi1Ac/edit?usp=sharing)

For a tutorial on using the API, please consult
[this Google doc](https://docs.google.com/document/d/1_3juAFAYl2bXaJEsPwKTxazcv2TwtST-QM8PXj5c2II/edit?usp=sharing).
""",
    version="2.0.0a",
    docs_url=None,
    redoc_url="/",
)

app.include_router(_general.router, tags=["General"])
app.include_router(_disorder.router, prefix="/disorder", tags=["Disorder"])
app.include_router(_ppi.router, tags=["PPI routes"])
app.include_router(_relations.router, prefix="/relations", tags=["Relations"])
