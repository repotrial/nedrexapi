from uuid import uuid4 as _uuid4

from neo4j import GraphDatabase as _GraphDatabase  # type: ignore
from pottery import Redlock, redis_cache

from nedrexapi.common import _REDIS
from nedrexapi.config import config
from nedrexapi.logger import logger

_NEWLINE = "\n"
_NEWLINE_TAB = "\n\t"
_NEO4J_DRIVER = _GraphDatabase.driver(uri=f"bolt://localhost:{config['db.dev.neo4j_bolt_port']}")


PPI_BASED_GGI_QUERY = """
MATCH (pa)-[ppi:ProteinInteractsWithProtein]-(pb)
WHERE "exp" in ppi.evidenceTypes
MATCH (pa)-[:ProteinEncodedByGene]->(x)
MATCH (pb)-[:ProteinEncodedByGene]->(y)
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

PPI_QUERY = """
MATCH (x)-[ppi:ProteinInteractsWithProtein]-(y)
WHERE "exp" in ppi.evidenceTypes
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

SHARED_DISORDER_BASED_GGI_QUERY = """
MATCH (x:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
MATCH (y:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
WHERE x <> y
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

QUERY_MAP = {
    ("gene", "DEFAULT"): PPI_BASED_GGI_QUERY,
    ("protein", "DEFAULT"): PPI_QUERY,
    ("gene", "SHARED_DISORDER"): SHARED_DISORDER_BASED_GGI_QUERY,
}


NETWORK_GEN_LOCK = Redlock(key="network_generation_lock", masters={_REDIS}, auto_release_time=int(1e10))


def get_network(query, prefix, type):
    logger.info(f"obtaining {type} network for query:{_NEWLINE_TAB}{query.strip().replace(_NEWLINE, _NEWLINE_TAB)}")
    with NETWORK_GEN_LOCK:
        logger.debug("obtained network generation lock")

        if type == "edge_list":
            network = get_network_edge_list(query, prefix)
            logger.info("network obtained")
            return network
        elif type == "sif":
            network = get_network_sif(query, prefix)
            logger.info("network obtained")
            return network
        else:
            raise Exception("invalid type given")


@redis_cache(redis=_REDIS, key="edge-list-generation-cache", timeout=int(1e10))
def get_network_edge_list(query, prefix):

    outfile = f"/tmp/{_uuid4()}.tsv"

    with _NEO4J_DRIVER.session() as session, open(outfile, "w") as f:
        for result in session.run(query):
            a = result["x.primaryDomainId"].replace(prefix, "")
            b = result["y.primaryDomainId"].replace(prefix, "")
            f.write("{}\t{}\n".format(a, b))

    return outfile


@redis_cache(redis=_REDIS, key="sif-generation-cache", timeout=int(1e10))
def get_network_sif(query, prefix):
    outfile = f"/tmp/{_uuid4()}.sif"

    edge_list = get_network_edge_list(query, prefix)
    with open(edge_list, "r") as f, open(outfile, "w") as g:
        for line in f:
            stripped_line = line.strip()
            if not stripped_line:
                continue
            a, b = stripped_line.split()
            g.write("{}\txx\t{}\n".format(a, b))

    return outfile


def normalise_seeds_and_determine_type(seeds):
    new_seeds = [seed.upper() for seed in seeds]

    if all(seed.startswith("ENTREZ.") for seed in new_seeds):
        seed_type = "gene"
        new_seeds = [seed.replace("ENTREZ.", "") for seed in new_seeds]
    elif all(seed.isnumeric() for seed in new_seeds):
        seed_type = "gene"
    elif all(seed.startswith("UNIPROT.") for seed in new_seeds):
        seed_type = "protein"
        new_seeds = [seed.replace("UNIPROT.", "") for seed in new_seeds]
    else:
        seed_type = "protein"

    return new_seeds, seed_type
