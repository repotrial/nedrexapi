from csv import DictWriter as _DictWriter
from io import StringIO as _StringIO
from typing import Optional

from cachetools import LRUCache as _LRUCache  # type: ignore
from cachetools import cached as _cached
from fastapi import APIRouter as _APIRouter
from fastapi import HTTPException as _HTTPException
from fastapi import Query as _Query
from fastapi import Response as _Response
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    EDGE_COLLECTIONS,
    NODE_COLLECTIONS,
    check_api_key_decorator,
)
from nedrexapi.config import config
from nedrexapi.db import MongoInstance

router = _APIRouter()


DEFAULT_QUERY = _Query(None)


@router.get(
    "/pagination_max",
    summary="Pagination limit",
)
@check_api_key_decorator
def pagination_maximum(x_api_key: str = _API_KEY_HEADER_ARG):
    """Returns the pagination maximum for the API"""
    return config["api.pagination_max"]


@router.get(
    "/api_key_setting",
    summary="API key setting",
)
def api_key_setting():
    """Returns true if API keys are required (and false otherwise)"""
    return config["api.require_api_keys"]


@router.get(
    "/list_node_collections",
    responses={200: {"content": {"application/json": {"example": ["disorder", "drug", "gene", "pathway", "protein"]}}}},
    summary="List node collections",
)
@check_api_key_decorator
def list_node_collections(x_api_key: str = _API_KEY_HEADER_ARG):
    return sorted(NODE_COLLECTIONS)


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
@check_api_key_decorator
def list_edge_collections(x_api_key: str = _API_KEY_HEADER_ARG):
    return sorted(EDGE_COLLECTIONS)


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
@check_api_key_decorator
def list_attributes(t: str, include_counts: bool = False, x_api_key: str = _API_KEY_HEADER_ARG):
    if t not in NODE_COLLECTIONS + EDGE_COLLECTIONS:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")

    data = MongoInstance.DB()["_collections"].find_one({"collection": t})

    if not data:
        raise _HTTPException(
            status_code=404,
            detail=(
                f"Collection attribute values are expecectedly not available for {t!r}"
                "(please raise an issue on GitHub)"
            ),
        )

    if include_counts:
        counts = data["attribute_counts"]

        if "_id" in counts:
            del counts["_id"]

        return {"document_count": data["document_count"], "attribute_counts": counts}

    attributes = data["unique_attributes"]
    attributes.remove("_id")
    return attributes


@router.get("/{t}/attributes/{attribute}/{format}", summary="Get attribute values")
@check_api_key_decorator
def get_attribute_values(t: str, attribute: str, format: str, x_api_key: str = _API_KEY_HEADER_ARG):
    if t in NODE_COLLECTIONS:
        results = [
            {"primaryDomainId": i["primaryDomainId"], attribute: i.get(attribute)} for i in MongoInstance.DB()[t].find()
        ]
    elif t in EDGE_COLLECTIONS:
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


class AttributeRequest(_BaseModel):
    node_ids: list[str] = _Field(None, title="Primary domain IDs of nodes", description="Primary domain IDs of the nodes the attributes are requested for")
    attributes: list[str] = _Field(None, title="Attributes requested", description="Attributes for which values are requested")

    class Config:
        extra = "forbid"

@router.post("/{t}/attributes/{format}", summary="Get for collection members selected attribute values")
@check_api_key_decorator
def get_attribute_values(t: str, format: str, ar: AttributeRequest = AttributeRequest(), x_api_key: str = _API_KEY_HEADER_ARG):
    if t not in NODE_COLLECTIONS:
        raise _HTTPException(
            status_code=404, detail=f"Collection {t!r} is not in the database"
        )

    if ar.attributes is None:
        raise _HTTPException(status_code=404, detail=f"No attribute(s) requested")
    if ar.node_ids is None:
        raise _HTTPException(status_code=404, detail=f"No node(s) requested")

    query = {"primaryDomainId": {"$in": ar.node_ids}}

    results = [
        {
            "primaryDomainId": i["primaryDomainId"],
            **{attribute: i.get(attribute) for attribute in ar.attributes},
        }
        for i in MongoInstance.DB()[t].find(query)
    ]

    if format == "json":
        return results

    elif format == "csv":
        string = _StringIO()
        keys = results[0].keys()
        dict_writer = _DictWriter(string, keys, delimiter=",")
        dict_writer.writeheader()
        dict_writer.writerows(results)
        return _Response(content=string.getvalue(), media_type="plain/text")

    elif format == "tsv":
        string = _StringIO()
        keys = results[0].keys()
        dict_writer = _DictWriter(string, keys, delimiter="\t")
        dict_writer.writeheader()
        dict_writer.writerows(results)
        return _Response(content=string.getvalue(), media_type="plain/text")
    # if t in NODE_COLLECTIONS:
    #     results = [
    #         {"primaryDomainId": i["primaryDomainId"], attribute: i.get(attribute)} for i in MongoInstance.DB()[t].find()
    #     ]
    # elif t in EDGE_COLLECTIONS:
    #     try:
    #         results = [
    #             {
    #                 "sourceDomainId": i["sourceDomainId"],
    #                 "targetDomainId": i["targetDomainId"],
    #                 attribute: i.get(attribute),
    #             }
    #             for i in MongoInstance.DB()[t].find()
    #         ]
    #     except KeyError:
    #         results = [
    #             {"memberOne": i["memberOne"], "memberTwo": i["memberTwo"], attribute: i.get(attribute)}
    #             for i in MongoInstance.DB()[t].find()
    #         ]
    # else:
    #     raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")
    #
    # if format == "json":
    #     return results
    # elif format in {"csv", "tsv"}:
    #     delimiter = "," if format == "csv" else "\t"
    #     string = _StringIO()
    #     keys = results[0].keys()
    #     dict_writer = _DictWriter(string, list(keys), delimiter=delimiter)
    #     dict_writer.writeheader()
    #     dict_writer.writerows(results)
    #     return _Response(content=string.getvalue(), media_type="plain/text")

@router.get("/{t}/attributes/{format}", summary="Get collection member attribute values")
@check_api_key_decorator
def get_node_attribute_values(
    t: str,
    format: str,
    attributes: list[str] = _Query(
        None,
        description=(
            "Attribute(s) requested. "
            "Multiple attributes can be specified (e.g., `attribute=domainIds&attribute=primaryDomainId)`"
        ),
        alias="attribute",
    ),
    node_ids: list[str] = _Query(
        None,
        description=(
            "Node IDs to collect attribute values for. "
            "Multiple node IDs can be specified (e.g., `node_id=<id_1>&node_id=<id_2>`)"
        ),
        alias="node_id",
    ),
    offset: Optional[int] = _Query(None, description="Offset to use"),
    limit: Optional[int] = _Query(
        None, description=f"Limit number of queries returned (default & maximum is {config['api.pagination_max']:,})"
    ),
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    # Singular is used for arguments because this makes sense to a user.
    # Aliasing to plural here as node_id and attribute are actually lists of 1+ strings.

    if t not in NODE_COLLECTIONS:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")
    if attributes is None:
        # get all attributes for the type
        attributes = list_attributes(t)

    if node_ids is None:
        query = {}
    else:
        query = {"primaryDomainId": {"$in": node_ids}}

    if limit is None:
        limit = config["api.pagination_max"]
    elif limit > config["api.pagination_max"]:
        raise _HTTPException(status_code=422, detail=f"Limit cannot be greater than {config['api.pagination_max']:,}")

    kwargs = {}
    if offset is not None:
        kwargs["skip"] = offset
    kwargs["limit"] = limit

    results = [
        {"primaryDomainId": i["primaryDomainId"], **{attr: i.get(attr) for attr in attributes}}
        for i in MongoInstance.DB()[t].find(query, **kwargs)
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
@check_api_key_decorator
def collection_details(t: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns a hash map of the details for the collection, `t`, including size (in bytes) and number of items.
    A collection a MongoDB concept that is analagous to a table in a RDBMS.
    """
    if t not in NODE_COLLECTIONS + EDGE_COLLECTIONS:
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
@check_api_key_decorator
def list_all_collection_items(t: str, offset: int = None, limit: int = None, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns an array of all items in the collection `t`.
    Items are returned as JSON, and have all of their attributes (and corresponding values).
    Note that this route may take a while to respond, depending on the size of the collection.
    """
    if t not in NODE_COLLECTIONS + EDGE_COLLECTIONS:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")

    if limit is None:
        limit = config["api.pagination_max"]
    elif limit > config["api.pagination_max"]:
        raise _HTTPException(status_code=422, detail=f"Limit cannot be greater than {config['api.pagination_max']:,}")

    kwargs = {}
    if offset is not None:
        kwargs["skip"] = offset
    kwargs["limit"] = limit

    return [{k: v for k, v in i.items() if k != "_id"} for i in MongoInstance.DB()[t].find(**kwargs)]


# Helper function for ID mapper
def get_primary_id(supplied_id, coll):
    result = list(MongoInstance.DB()[coll].find({"domainIds": supplied_id}))
    if result:
        return [i["primaryDomainId"] for i in result]


@router.get("/get_by_id/{t}", summary="Get by ID")
@check_api_key_decorator
def get_by_id(t: str, q: list[str] = DEFAULT_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns an array of items with one or more of the specified query IDs, `q`, from a collection, `t`.
    The query IDs are of the form `{database}.{accession}`, for example `uniprot.Q9UBT6`.
    Note that the query IDs can be a combination of (1) primary domain ID and (2) any other domain ID used to refer
    to an entity (e.g., `mondo.0020066` and `ncit.C92622` in the above example).
    """
    if not q:
        return []

    if t not in NODE_COLLECTIONS:
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
@check_api_key_decorator
def id_map(t: str, q: list[str] = DEFAULT_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
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

    if t not in NODE_COLLECTIONS:
        raise _HTTPException(status_code=404, detail=f"Collection {t!r} is not in the database")
    result = {item: get_primary_id(item, t) for item in q}
    return result
