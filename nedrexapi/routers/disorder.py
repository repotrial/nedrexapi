from collections import defaultdict as _defaultdict

import networkx as _nx  # type: ignore
from cachetools import LRUCache as _LRUCache  # type: ignore
from cachetools import cached as _cached
from fastapi import APIRouter as _APIRouter
from fastapi import Query as _Query

from nedrexapi.common import _API_KEY_HEADER_ARG, check_api_key_decorator
from nedrexapi.db import MongoInstance

DEFAULT_QUERY = _Query(None)

router = _APIRouter()


@_cached(cache=_LRUCache(maxsize=1))
def construct_disorder_relationship_graph():
    g = _nx.DiGraph()
    for i in MongoInstance.DB()["disorder"].find():
        g.add_node(i["primaryDomainId"])
    for i in MongoInstance.DB()["disorder_is_subtype_of_disorder"].find():
        g.add_edge(i["sourceDomainId"], i["targetDomainId"])
    return g


@router.get("/get_by_icd10", summary="Get disorder(s) by ICD-10")
@check_api_key_decorator
def get_disorder_by_icd10(q: list[str] = DEFAULT_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns an array of disorders with the given ICD-10 codes.
    Both 3 character and 4 character codes can be used (including in the same query).
    """
    if not q:
        return []

    hits = MongoInstance.DB()["disorder"].find({"icd10": {"$in": q}})
    hits = [{k: v for k, v in i.items() if k != "_id"} for i in hits]
    return hits


@router.get(
    "/descendants",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "mondo.0005252": [
                            "mondo.0006727",
                            "mondo.0004595",
                            "mondo.0001493",
                            "mondo.0005253",
                            "mondo.0001899",
                            "mondo.0009935",
                            "mondo.0004598",
                            "mondo.0005254",
                            "mondo.0005255",
                            "mondo.0006993",
                            "mondo.0005257",
                            "mondo.0001999",
                            "mondo.0014136",
                            "mondo.0017148",
                            "mondo.0001492",
                            "mondo.0008347",
                            "mondo.0014134",
                            "mondo.0005009",
                            "mondo.0017147",
                            "mondo.0004597",
                            "mondo.0024533",
                            "mondo.0005256",
                            "mondo.0004596",
                            "mondo.0014135",
                            "mondo.0044079",
                        ]
                    }
                }
            }
        }
    },
    summary="Get disorder descendants",
)
@check_api_key_decorator
def get_disorder_descendants(q: list[str] = DEFAULT_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the descendant disorder terms for the given disorder term(s).
    Results are returned as a hash map of `{child: descendants}`.
    Note that you can use any domain ID (e.g., `omim.130020`) to query, but the IDs used in the returned hash map will
    be returned as MONDO IDs.
    An example of a descendant relationship would be Heart failure (`mondo.0005252`) has descendant Pulmonary
    hypertension, primary, autosomal recessive (`mondo.0009935`).
    """
    if not q:
        return {}
    g = construct_disorder_relationship_graph()
    hits = MongoInstance.DB()["disorder"].find({"domainIds": {"$in": q}})
    hits = [i["primaryDomainId"] for i in hits]

    results = {hit: sorted(_nx.algorithms.dag.ancestors(g, hit)) for hit in hits}
    return results


@router.get(
    "/ancestors",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "mondo.0005252": [
                            "mondo.0005267",
                            "mondo.0004995",
                            "mondo.0000651",
                            "mondo.0024505",
                            "mondo.0021199",
                            "mondo.0000001",
                        ]
                    }
                }
            }
        }
    },
    summary="Get disorder ancestors",
)
@check_api_key_decorator
def get_disorder_ancestors(q: list[str] = DEFAULT_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the ancestor disorder terms for the given disorder term(s).
    Results are returned as a hash map of `{child: ancestors}`.
    Note that you can use any domain ID (e.g., `omim.130020`) to query, but the IDs used in the returned hash map will
    be returned as MONDO IDs.
    An example of an ancestor relationship would be Ehlers-Danlos syndrome, hypermobility type (`mondo.0007523`) has
    ancestor Disease or Disorder (`mondo.0000001`),
    the root term of the Monarch Disease Ontology.
    """
    if not q:
        return {}

    g = construct_disorder_relationship_graph()

    # First query, check disorder(s) exist
    hits = MongoInstance.DB()["disorder"].find({"domainIds": {"$in": q}})
    hits = [i["primaryDomainId"] for i in hits]

    # We use the descendants method due to the direction of the relationships (point up the tree, therefore "children"
    # are terms higher up the tree)
    results = {hit: sorted(_nx.algorithms.dag.descendants(g, hit)) for hit in hits}
    return results


@router.get(
    "/parents",
    responses={200: {"content": {"application/json": {"example": {"mondo.0007523": ["mondo.0020066"]}}}}},
    summary="Get disorder parents",
)
@check_api_key_decorator
def get_disorder_parents(q: list[str] = DEFAULT_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the parent disorder terms for the given disorder term(s).
    Results are returned as a hash map of `{child: parents}`.
    Note that you can use any domain ID (e.g., `omim.130020`) to query, but the IDs used in the returned hash map will
    be returned as MONDO IDs.
    An example of a parent relationship is Ehlers-Danlos syndrome, hypermobility type (`mondo.0007523`) has parent
    Ehlers-Danlos syndrome (`mondo.0020066`).
    """
    if not q:
        return {}

    # First query, check the disorder(s) exists.
    hits = MongoInstance.DB()["disorder"].find({"domainIds": {"$in": q}})
    hits = [i["primaryDomainId"] for i in hits]

    # Get parents.
    diad_coll = MongoInstance.DB()["disorder_is_subtype_of_disorder"]
    results = diad_coll.find({"sourceDomainId": {"$in": hits}})

    # Format a dictionary in the form {child: parents}
    return_dct: dict[str, list[str]] = _defaultdict(list)
    for doc in results:
        return_dct[doc["sourceDomainId"]].append(doc["targetDomainId"])

    return_dct = {k: sorted(v) for k, v in return_dct.items()}

    return return_dct


@router.get(
    "/children/",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "mondo.0020066": [
                            "mondo.0002254",
                            "mondo.0019292",
                            "mondo.0015332",
                            "mondo.0015331",
                            "mondo.0017133",
                            "mondo.0024255",
                            "mondo.0015938",
                        ]
                    }
                }
            }
        }
    },
    summary="Get disorder children",
)
@check_api_key_decorator
def get_disorder_children(q: list[str] = DEFAULT_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the children disorder terms for the given disorder term(s).
    Results are returned as a hash map of `{parent: children}`.
    Note that you can use any domain ID (e.g., `omim.130020`) to query, but the IDs used in the returned hash map will
    be returned as MONDO IDs.
    An example of a parent relationship is Ehlers-Danlos syndrome (`mondo.0020066`) has (among others) child
    Ehlers-Danlos syndrome, hypermobility type (`mondo.0007523`).
    """

    # First query, check the disorder exists.
    hits = MongoInstance.DB()["disorder"].find({"domainIds": {"$in": q}})
    hits = [i["primaryDomainId"] for i in hits]

    # Get children.
    diad_coll = MongoInstance.DB()["disorder_is_subtype_of_disorder"]
    results = diad_coll.find({"targetDomainId": {"$in": hits}})

    # Format a dictionary in the form {child: parents}
    return_dct: dict[str, list[str]] = _defaultdict(list)
    for doc in results:
        return_dct[doc["targetDomainId"]].append(doc["sourceDomainId"])

    return_dct = {k: sorted(v) for k, v in return_dct.items()}

    return return_dct
