import json

from fastapi import APIRouter as _APIRouter
from fastapi.responses import StreamingResponse
from more_itertools import chunked
from neo4j import GraphDatabase as _GraphDatabase  # type: ignore


from nedrexapi.config import config as _config

_NEO4J_DRIVER = _GraphDatabase.driver(uri=f"bolt://localhost:{_config['db.neo4j_bolt_port']}")

router = _APIRouter()


async def run_query(query):
    with _NEO4J_DRIVER.session() as session:
        result = session.run(query)
        for chunk in chunked(result, 1_000):
            yield json.dumps([i.data() for i in chunk]) + "\n"


@router.get("/query", summary="Neo4j query")
def neo4j_query(query: str):
    """
    Runs a Neo4j query and returns the result.

    The result is returned as a streaming response, so it is up to the user to handle the streaming response.
    An example of this using Python's requests library is below:

        import json
        import requests
        query = "MATCH (n) RETURN n LIMIT 25"
        url = "http://82.148.225.92:8022/neo4j/query"

        response = requests.get(url, params={"query":query}, stream=True)
        for line in response.iter_lines():
            print(json.loads(line.decode()))
    """
    return StreamingResponse(run_query(query))
