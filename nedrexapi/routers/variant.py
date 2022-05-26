from typing import Optional

from fastapi import APIRouter as _APIRouter, HTTPException as _HTTPException, Query as _Query
from pottery import synchronize, RedisDict

from nedrexapi.common import check_api_key_decorator, _API_KEY_HEADER_ARG, _REDIS
from nedrexapi.db import MongoInstance

router = _APIRouter()

_VARIANT_ROUTE_CHOICES = RedisDict({}, redis=_REDIS, key="variant-route-choices")


@synchronize(masters={_REDIS}, key="variant-effect-choices-sync", auto_release_time=int(1e10))
def _get_effect_choices():
    if _VARIANT_ROUTE_CHOICES.get("effects"):
        return _VARIANT_ROUTE_CHOICES["effects"]

    effect_choices = set()
    for vda in MongoInstance.DB()["variant_associated_with_disorder"].find():
        effect_choices.update(vda["effects"])

    _VARIANT_ROUTE_CHOICES["effects"] = sorted(effect_choices)
    return _VARIANT_ROUTE_CHOICES["effects"]


@router.get("/get_effect_choices", summary="Get effect choices")
@check_api_key_decorator
def get_effect_choices(x_api_key: str = _API_KEY_HEADER_ARG):
    return _get_effect_choices()


@synchronize(masters={_REDIS}, key="variant-review-status-choices-sync", auto_release_time=int(1e10))
def _get_review_statuses():
    if _VARIANT_ROUTE_CHOICES.get("review_statuses"):
        return _VARIANT_ROUTE_CHOICES["review_statuses"]

    statuses = {vad["reviewStatus"] for vad in MongoInstance.DB()["variant_associated_with_disorder"].find()}
    _VARIANT_ROUTE_CHOICES["review_statuses"] = sorted(statuses)
    return _VARIANT_ROUTE_CHOICES["review_statuses"]


@router.get("/get_review_choices", summary="Get review status choices")
@check_api_key_decorator
def get_review_statuses(x_api_key: str = _API_KEY_HEADER_ARG):
    return _get_review_statuses()


@router.get("/get_variant_disorder_associations", summary="Get variant-disorder associations")
@check_api_key_decorator
def get_variant_disorder_associations(
    variant_ids: Optional[list[str]] = _Query(
        None,
        title="Variant IDs to get variant-disorder relationships for",
        description="Default: `None` (no filtering on variant IDs)",
        alias="variant_id",
    ),
    disorder_ids: Optional[list[str]] = _Query(
        None,
        title="Disorder IDs to get variant-disorder relationships for",
        description="Default: `None` (no filtering on disorder IDs)",
        alias="disorder_id",
    ),
    review_status: Optional[list[str]] = _Query(
        None,
        title="Review status(es) to include variant disorder relationships for",
        description="Default: `['practice guideline', 'reviewed by expert panel']`",
        alias="review_status",
    ),
    effects: Optional[list[str]] = _Query(
        None,
        title="Effect(s) to include variant-disorder relationships for",
        description="Default: `['Pathogenic', 'Likely pathogenic', 'Pathogenic/Likely pathogenic']`",
        alias="effect",
    ),
    x_api_key: str = _API_KEY_HEADER_ARG,
):

    query = {}
    if variant_ids is not None:
        query["sourceDomainId"] = {"$in": variant_ids}
    if disorder_ids is not None:
        query["targetDomainId"] = {"$in": disorder_ids}

    if review_status is None:
        query["reviewStatus"] = {"$in": ["practice guideline", "reviewed by expert panel"]}
    else:
        query["reviewStatus"] = {"$in": review_status}

    if effects is None:
        query["effects"] = {"$in": ["Pathogenic", "Likely pathogenic", "Pathogenic/Likely pathogenic"]}
    else:
        query["effects"] = {"$in": effects}

    results = list(MongoInstance.DB()["variant_associated_with_disorder"].find(query))
    [i.pop("_id") for i in results]
    return results


@router.get("/get_variant_gene_associations", summary="Get variant-gene associations")
@check_api_key_decorator
def get_variant_gene_associations(
    variant_ids: Optional[list[str]] = _Query(
        None,
        title="Variant IDs to get variant-gene relationships for",
        description="Default: `None` (no filtering on variant IDs)",
        alias="variant_id",
    ),
    gene_ids: Optional[list[str]] = _Query(
        None,
        title="Gene IDs to get variant-gene relationships for",
        description="Default: `None` (no filtering on gene IDs)",
        alias="gene_id",
    ),
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    """
    Gets the variant-gene (V-G) relationships associated with the requested variant(s)/gene(s).

    Note that this function behaves as an AND with respect to the inputs.
    This means that if you specify variant IDs and gene IDs then you will get VG relationships where the variant is
    one of the variant IDs specified and the gene is one of the gene IDs specified.
    """

    query = {}
    if variant_ids is not None:
        query["sourceDomainId"] = {"$in": variant_ids}
    if gene_ids is not None:
        query["targetDomainId"] = {"$in": gene_ids}

    results = list(MongoInstance.DB()["variant_affects_gene"].find(query))
    [i.pop("_id") for i in results]
    return results


@router.get("/variant_based_disorder_associated_genes", summary="Get variant-based genes associated with disorder")
@check_api_key_decorator
def variant_based_genes_associated_with_disorder(
    disorder_id: str = _Query(
        None,
        title="Disorder IDs to get variant-disorder relationships for",
        description="Default: `None` (no filtering on disorder IDs)",
        alias="disorder_id",
    ),
    review_status: Optional[list[str]] = _Query(
        None,
        title="Review status(es) to include variant disorder relationships for",
        description="Default: `['practice guideline', 'reviewed by expert panel']`",
    ),
    effects: Optional[list[str]] = _Query(
        None,
        title="Effect(s) to include variant-disorder relationships for",
        description="Default: `['Pathogenic', 'Likely pathogenic', 'Pathogenic/Likely pathogenic']`",
        alias="effect",
    ),
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    """
    Identifies genes associated with a disorder, using variants as an intermediary.
    """
    if disorder_id is None:
        raise _HTTPException(status_code=404, detail="No disorder ID specified")

    variant_ids = [
        doc["sourceDomainId"]
        for doc in get_variant_disorder_associations(
            variant_ids=None, disorder_ids=[disorder_id], review_status=review_status, effects=effects
        )
    ]

    return sorted(set(doc["targetDomainId"] for doc in get_variant_gene_associations(variant_ids, gene_ids=None)))


@router.get("/variant_based_gene_associated_disorders", summary="Get variant-based disorders associated with a gene")
@check_api_key_decorator
def variant_based_disorders_associated_with_gene(
    gene_id: str = _Query(
        None,
        title="Disorder IDs to get variant-disorder relationships for",
        description="Default: `None` (no filtering on disorder IDs)",
    ),
    review_status: Optional[list[str]] = _Query(
        None,
        title="Review status(es) to include variant disorder relationships for",
        description="Default: `['practice guideline', 'reviewed by expert panel']`",
    ),
    effects: Optional[list[str]] = _Query(
        None,
        title="Effect(s) to include variant-disorder relationships for",
        description="Default: `['Pathogenic', 'Likely pathogenic', 'Pathogenic/Likely pathogenic']`",
        alias="effect",
    ),
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    """
    Searches NeDRexDB for disorders associated with a gene, using variants as an intermediary.
    """
    if gene_id is None:
        raise _HTTPException(status_code=404, detail="No gene ID specified")

    variant_ids = [
        doc["sourceDomainId"]
        for doc in get_variant_gene_associations(
            gene_ids=[gene_id],
            variant_ids=None,
        )
    ]

    return sorted(
        set(
            doc["targetDomainId"]
            for doc in get_variant_disorder_associations(
                variant_ids=variant_ids,
                review_status=review_status,
                effects=effects,
                disorder_ids=None,
            )
        )
    )
