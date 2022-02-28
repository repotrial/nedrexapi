from csv import DictWriter as _DictWriter
from io import StringIO as _StringIO

from cachetools import LRUCache as _LRUCache, cached as _cached  # type: ignore
from fastapi import APIRouter as _APIRouter, HTTPException as _HTTPException, Response as _Response, Query as _Query
from pydantic import BaseModel as _BaseModel, Field as _Field

from nedrexapi.db import MongoInstance
from nedrexapi.config import config
from nedrexapi.routers.admin import check_api_key

router = _APIRouter()


class AttributeRequest(_BaseModel):
    node_ids: list[str] = _Field(None, title="Primary domain IDs of nodes")
    attributes: list[str] = _Field(None, title="Attributes requested")
    api_key: str = _Field(None, title="API key (only required to access some data")

    class Config:
        extra = "forbid"


DEFAULT_ATTRIBUTE_REQUEST = AttributeRequest()
DEFAULT_QUERY = _Query(None)


@router.get(
    "/list_node_collections",
    responses={200: {"content": {"application/json": {"example": ["disorder", "drug", "gene", "pathway", "protein"]}}}},
    summary="List node collections",
)
def list_node_collections():
    return config["api.node_collections"]


@router.get(
    "/list_edge_collections",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": [
                        "disorder_has_phenotype",
                        "disorder_is_subtype_of_disorder",
                        "drug_has_contraindication",
                        "drug_has_indication",
                        "drug_has_target",
                        "gene_associated_with_disorder",
                        "go_is_subtype_of_go",
                        "protein_encoded_by_gene",
                        "protein_has_go_annotation",
                        "protein_in_pathway",
                        "protein_interacts_with_protein",
                    ]
                }
            }
        }
    },
    summary="List edge collections",
)
def list_edge_collections():
    return config["api.edge_collections"]


@router.get(
    "/{t}/attributes",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": [
                        "synonyms",
                        "domainIds",
                        "primaryDomainId",
                        "type",
                        "displayName",
                        "comments",
                        "taxid",
                        "sequence",
                        "geneName",
                    ]
                }
            }
        },
        404: {"content": {"application/json": {"example": {"detail": "Collection 'tissue' is not in the database"}}}},
    },
    summary="List collection attributes",
)
@_cached(cache=_LRUCache(maxsize=32))
def list_attributes(t: str):
    if t not in config["api.node_collections"] + config["api.edge_collections"]:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")

    attributes: set[str] = set()
    for doc in MongoInstance.DB()[t].find():
        attributes |= set(doc.keys())
    attributes.remove("_id")
    return attributes


@router.get("/{t}/attributes/{attribute}/{format}")
def get_attribute_values(t: str, attribute: str, format: str, api_key: str = None):
    if t in {"drug", "drug_has_target", "gene_associated_with_disorder"}:
        check_api_key(api_key)

    if t in config["api.node_collections"]:
        results = [
            {"primaryDomainId": i["primaryDomainId"], attribute: i.get(attribute)} for i in MongoInstance.DB()[t].find()
        ]
    elif t in config["api.edge_collections"]:
        try:
            results = [
                {
                    "sourceDomainId": i["sourceDomainId"],
                    "targetDomainId": i["targetDomainId"],
                    attribute: i.get(attribute),
                }
                for i in MongoInstance.DB()[t].find()
            ]
        except KeyError:
            results = [
                {"memberOne": i["memberOne"], "memberTwo": i["memberTwo"], attribute: i.get(attribute)}
                for i in MongoInstance.DB()[t].find()
            ]
    else:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")

    if format == "json":
        return results
    elif format in {"csv", "tsv"}:
        delimiter = "," if format == "csv" else "\t"
        string = _StringIO()
        keys = results[0].keys()
        dict_writer = _DictWriter(string, list(keys), delimiter=delimiter)
        dict_writer.writeheader()
        dict_writer.writerows(results)
        return _Response(content=string.getvalue(), media_type="plain/text")


@router.get("/{t}/attributes_v2/{format}", summary="Get collection member attribute values")
def get_node_attribute_values(t: str, format: str, ar: AttributeRequest = DEFAULT_ATTRIBUTE_REQUEST):
    if t not in config.get("api.node_collections"):
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")
    if ar.attributes is None:
        raise _HTTPException(status_code=404, detail="No attribute(s) requested")
    if ar.node_ids is None:
        raise _HTTPException(status_code=404, detail="No node(s) requested")

    # NOTE: Only need a special case for drugs because this route only gives access to nodes (not edges).
    if t == "drug":
        check_api_key(ar.api_key)

    query = {"primaryDomainId": {"$in": ar.node_ids}}

    results = [
        {"primaryDomainId": i["primaryDomainId"], **{attribute: i.get(attribute) for attribute in ar.attributes}}
        for i in MongoInstance.DB()[t].find(query)
    ]

    if format == "json":
        return results
    elif format in {"csv", "tsv"}:
        delimiter = "," if format == "csv" else "\t"
        string = _StringIO()
        keys = results[0].keys()
        dict_writer = _DictWriter(string, list(keys), delimiter=delimiter)
        dict_writer.writeheader()
        dict_writer.writerows(results)
        return _Response(content=string.getvalue(), media_type="plain/text")


@router.get(
    "/{t}/details",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "ns": "test.drug",
                        "size": 16029934,
                        "count": 13300,
                        "avgObjSize": 1205,
                        "storageSize": 8798208,
                        "capped": False,
                        "nindexes": 3,
                        "totalIndexSize": 557056,
                        "indexSizes": {"_id_": 167936, "primaryDomainId_1": 278528, "_cls_1": 110592},
                        "ok": 1.0,
                    }
                }
            }
        },
        404: {"content": {"application/json": {"example": {"detail": "Collection 'tissue' is not in the database"}}}},
    },
    summary="Collection details",
)
@_cached(cache=_LRUCache(maxsize=32))
def collection_details(t: str):
    """
    Returns a hash map of the details for the collection, `t`, including size (in bytes) and number of items.
    A collection a MongoDB concept that is analagous to a table in a RDBMS.
    """
    if t not in config["api.node_collections"] + config["api.edge_collections"]:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")

    result = MongoInstance.DB().command("collstats", t)
    return {k: v for k, v in result.items() if k not in ["wiredTiger", "indexDetails"]}


@router.get(
    "/{t}/all",
    responses={
        200: {"content": {"application/json": {}}},
        404: {"content": {"application/json": {"example": {"detail": "Collection 'tissue' is not in the database"}}}},
    },
    summary="List all collection items",
)
@_cached(cache=_LRUCache(maxsize=32))
def list_all_collection_items(t: str, api_key: str = None):
    """
    Returns an array of all items in the collection `t`.
    Items are returned as JSON, and have all of their attributes (and corresponding values).
    Note that this route may take a while to respond, depending on the size of the collection.
    """
    if t not in config["api.node_collections"] + config["api.edge_collections"]:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")

    if t in {"drug", "drug_has_target", "gene_associated_with_disorder"}:
        check_api_key(api_key)

    return [{k: v for k, v in i.items() if k != "_id"} for i in MongoInstance.DB()[t].find()]


# Helper function for ID mapper
def get_primary_id(supplied_id, coll):
    result = list(MongoInstance.DB()[coll].find({"domainIds": supplied_id}))
    if result:
        return [i["primaryDomainId"] for i in result]


@router.get("/get_by_id/{t}", summary="Get by ID")
def get_by_id(t: str, q: list[str] = DEFAULT_QUERY):
    """
    Returns an array of items with one or more of the specified query IDs, `q`, from a collection, `t`.
    The query IDs are of the form `{database}.{accession}`, for example `uniprot.Q9UBT6`.
    Note that the query IDs can be a combination of (1) primary domain ID and (2) any other domain ID used to refer
    to an entity (e.g., `mondo.0020066` and `ncit.C92622` in the above example).
    """
    if not q:
        return []

    if t not in config["api.node_collections"]:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")

    result = MongoInstance.DB()[t].find({"domainIds": {"$in": q}})
    result = [{k: v for k, v in i.items() if not k == "_id"} for i in result]
    return result


@router.get(
    "/id_map/{t}",
    responses={
        200: {"content": {"application/json": {}}},
        404: {"content": {"application/json": {"example": {"detail": "Collection 'tissue' is not in the database"}}}},
    },
    summary="ID map",
)
def id_map(t: str, q: list[str] = DEFAULT_QUERY):
    """
    Returns a hash map of `{user-supplied-id: [primaryDomainId]}` for a set of user-specified identifiers in a
    user-specified collection, `t`.
    The values in the hash map are an array because, rarely, integrated databases (e.g., MONDO) map a single external
    identifier onto two nodes.
    An array is returned so that the choice of how to handle this is in control of the client.
    """
    # If the user supplied no query parameters.
    if not q:
        return {}

    if t not in config["api.node_collections"]:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")
    result = {item: get_primary_id(item, t) for item in q}
    return result
