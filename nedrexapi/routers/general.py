from cachetools import LRUCache as _LRUCache, cached as _cached  # type: ignore
from fastapi import APIRouter as _APIRouter, HTTPException as _HTTPException
from pydantic import BaseModel as _BaseModel, Field as _Field

from nedrexapi.db import MongoInstance
from nedrexapi.config import config

router = _APIRouter()


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


class AttributeRequest(_BaseModel):
    node_ids: list[str] = _Field(None, title="Primary domain IDs of nodes")
    attributes: list[str] = _Field(None, title="Attributes requested")

    class Config:
        extra = "forbid"


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
    assert MongoInstance.DB is not None

    attributes: set[str] = set()
    for doc in MongoInstance.DB[t].find():
        attributes |= set(doc.keys())
    attributes.remove("_id")
    return attributes
