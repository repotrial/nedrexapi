from fastapi import APIRouter as _APIRouter, HTTPException as _HTTPException

from pottery import synchronize, RedisDict
from pydantic import BaseModel as _BaseModel, Field as _Field

from nedrexapi.common import _REDIS
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
def get_effect_choices():
    return _get_effect_choices()


@synchronize(masters={_REDIS}, key="variant-review-status-choices-sync", auto_release_time=int(1e10))
def _get_review_statuses():
    if _VARIANT_ROUTE_CHOICES.get("review_statuses"):
        return _VARIANT_ROUTE_CHOICES["review_statuses"]

    statuses = {vad["reviewStatus"] for vad in MongoInstance.DB()["variant_associated_with_disorder"].find()}
    _VARIANT_ROUTE_CHOICES["review_statuses"] = sorted(statuses)
    return _VARIANT_ROUTE_CHOICES["review_statuses"]


@router.get("/get_review_choices", summary="Get review status choices")
def get_review_statuses():
    return _get_review_statuses()


# VDA: Variant Disorder Association
class VDAFilter(_BaseModel):
    variant_ids: list[str] = _Field(
        None,
        title="Variant IDs to get variant-disorder relationships for",
        description="Default: `None` (no filtering on variant IDs)",
    )
    disorder_ids: list[str] = _Field(
        None,
        title="Disorder IDs to get variant-disorder relationships for",
        description="Default: `None` (no filtering on disorder IDs)",
    )
    review_status: list[str] = _Field(
        None,
        title="Review status(es) to include variant disorder relationships for",
        description="Default: `['practice guideline', 'reviewed by expert panel']`",
    )
    effects: list[str] = _Field(
        None,
        title="Effect(s) to include variant-disorder relationships for",
        description="Default: `['Pathogenic', 'Likely pathogenic', 'Pathogenic/Likely pathogenic']`",
    )


DEFAULT_VDA_FILTER = VDAFilter()


@router.get("/get_variant_disorder_associations", summary="Get variant-disorder associations")
def get_variant_disorder_associations(vda_filter: VDAFilter = DEFAULT_VDA_FILTER):
    query = {}
    if vda_filter.variant_ids is not None:
        query["sourceDomainId"] = {"$in": vda_filter.variant_ids}
    if vda_filter.disorder_ids is not None:
        query["targetDomainId"] = {"$in": vda_filter.disorder_ids}

    if vda_filter.review_status is None:
        query["reviewStatus"] = {"$in": ["practice guideline", "reviewed by expert panel"]}
    else:
        query["reviewStatus"] = {"$in": vda_filter.review_status}

    if vda_filter.effects is None:
        query["effects"] = {"$in": ["Pathogenic", "Likely pathogenic", "Pathogenic/Likely pathogenic"]}
    else:
        query["effects"] = {"$in": vda_filter.effects}

    results = list(MongoInstance.DB()["variant_associated_with_disorder"].find(query))
    [i.pop("_id") for i in results]
    return results


# VGA: Variant Gene Association
class VGAFilter(_BaseModel):
    variant_ids: list[str] = _Field(
        None,
        title="Variant IDs to get variant-gene relationships for",
        description="Default: `None` (no filtering on variant IDs)",
    )
    gene_ids: list[str] = _Field(
        None,
        title="Gene IDs to get variant-gene relationships for",
        description="Default: `None` (no filtering on gene IDs)",
    )


DEFAULT_VGA_FILTER = VGAFilter()


@router.get("/get_variant_gene_associations", summary="Get variant-gene associations")
def get_variant_gene_associations(vga_filter: VGAFilter = DEFAULT_VGA_FILTER):
    query = {}
    if vga_filter.variant_ids is not None:
        query["sourceDomainId"] = {"$in": vga_filter.variant_ids}
    if vga_filter.gene_ids is not None:
        query["targetDomainId"] = {"$in": vga_filter.gene_ids}

    results = list(MongoInstance.DB()["variant_affects_gene"].find(query))
    [i.pop("_id") for i in results]
    return results


# VDA: Variant-based disease-associated genes
class VariantBasedDAG(_BaseModel):
    disorder_id: str = _Field(
        None,
        title="Disorder IDs to get variant-disorder relationships for",
        description="Default: `None` (no filtering on disorder IDs)",
    )
    review_status: list[str] = _Field(
        None,
        title="Review status(es) to include variant disorder relationships for",
        description="Default: `['practice guideline', 'reviewed by expert panel']`",
    )
    effects: list[str] = _Field(
        None,
        title="Effect(s) to include variant-disorder relationships for",
        description="Default: `['Pathogenic', 'Likely pathogenic', 'Pathogenic/Likely pathogenic']`",
    )


DEFAULT_VARIANT_BASED_DAG_QUERY = VariantBasedDAG()


@router.get("/variant_based_disorder_associated_genes", summary="Get variant-based genes associated with disorder")
def variant_based_genes_associated_with_disorder(query: VariantBasedDAG = DEFAULT_VARIANT_BASED_DAG_QUERY):
    """
    Identifies genes associated with a disorder, using variants as an intermediary.
    """
    variant_disorder_filter = VDAFilter()

    if query.disorder_id is None:
        raise _HTTPException(status_code=404, detail="No disorder ID specified")
    variant_disorder_filter.disorder_ids = [query.disorder_id]

    if query.review_status is not None:
        variant_disorder_filter.review_status = query.review_status
    if query.effects is not None:
        variant_disorder_filter.effects = query.effects

    variant_ids = [doc["sourceDomainId"] for doc in get_variant_disorder_associations(variant_disorder_filter)]

    variant_gene_filter = VGAFilter(variant_ids=variant_ids)

    return sorted(set(doc["targetDomainId"] for doc in get_variant_gene_associations(variant_gene_filter)))


# Variant-based gene-associated disorders
class VariantBasedGAD(_BaseModel):
    gene_id: str = _Field(
        None,
        title="Disorder IDs to get variant-disorder relationships for",
        description="Default: `None` (no filtering on disorder IDs)",
    )
    review_status: list[str] = _Field(
        None,
        title="Review status(es) to include variant disorder relationships for",
        description="Default: `['practice guideline', 'reviewed by expert panel']`",
    )
    effects: list[str] = _Field(
        None,
        title="Effect(s) to include variant-disorder relationships for",
        description="Default: `['Pathogenic', 'Likely pathogenic', 'Pathogenic/Likely pathogenic']`",
    )


DEFAULT_VARIANT_BASED_GAD_QUERY = VariantBasedGAD()


@router.get("/variant_based_gene_associated_disorders", summary="Get variant-based disorders associated with a gene")
def variant_based_disorders_associated_with_gene(query: VariantBasedGAD = DEFAULT_VARIANT_BASED_GAD_QUERY):
    """
    Searches NeDRexDB for disorders associated with a gene, using variants as an intermediary.
    """

    if query.gene_id is None:
        raise _HTTPException(status_code=404, detail="No gene ID specified")
    variant_gene_filter = VGAFilter(gene_ids=[query.gene_id])

    variant_ids = [doc["sourceDomainId"] for doc in get_variant_gene_associations(variant_gene_filter)]

    variant_disorder_filter = VDAFilter()
    variant_disorder_filter.variant_ids = variant_ids

    if query.review_status is not None:
        variant_disorder_filter.review_status = query.review_status
    if query.effects is not None:
        variant_disorder_filter.effects = query.effects

    return sorted(set(doc["targetDomainId"] for doc in get_variant_disorder_associations(variant_disorder_filter)))
