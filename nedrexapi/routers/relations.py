from itertools import chain as _chain

from fastapi import APIRouter as _APIRouter
from fastapi import Query as _Query

from nedrexapi.common import _API_KEY_HEADER_ARG, check_api_key_decorator
from nedrexapi.db import MongoInstance

router = _APIRouter()

GENE_QUERY = _Query(
    None,
    title="Genes",
    description=(
        "Gene(s) to get relationships for. " "Multiple genes can be specified (e.g., `gene=entrez.1&gene=entrez.2`)"
    ),
    alias="gene",
)

DISORDER_QUERY = _Query(
    None,
    title="Disorders",
    description=(
        "Disorder(s) to get relationships for. "
        "Multiple disorders can be specified (e.g., `disorder=mondo.0005252&disorder=mondo.0006727`"
    ),
    alias="disorder",
)

PROTEIN_QUERY = _Query(
    None,
    title="Proteins",
    description=(
        "Protein(s) to get relationships for. "
        "Multiple proteins can be specified (e.g., `protein=uniprot.P51451&protein=uniprot.A6H8Y1`"
    ),
    alias="protein",
)


@router.get("/get_encoded_proteins")
@check_api_key_decorator
def get_encoded_proteins(genes: list[str] = GENE_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Given a set of seed genes, this route returns the proteins encoded by those genes as a hash map.
    """
    genes = [f"entrez.{i}" if not i.startswith("entrez") else i for i in genes]

    coll = MongoInstance.DB()["protein_encoded_by_gene"]
    query = {"targetDomainId": {"$in": genes}}

    # NOTE: This ensures that all query disorders appear in the results
    results: dict[str, list[str]] = {gene.replace("entrez.", ""): [] for gene in genes}

    for doc in coll.find(query):
        gene = doc["targetDomainId"].replace("entrez.", "")
        protein = doc["sourceDomainId"].replace("uniprot.", "")
        results[gene].append(protein)

    return results


@router.get("/get_drugs_indicated_for_disorders")
@check_api_key_decorator
def get_drugs_indicated_for_disorders(disorders: list[str] = DISORDER_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    disorders = [f"mondo.{i}" if not i.startswith("mondo") else i for i in disorders]

    coll = MongoInstance.DB()["drug_has_indication"]
    query = {"targetDomainId": {"$in": disorders}}

    # NOTE: This ensures that all query disorders appear in the results
    results: dict[str, list[str]] = {disorder.replace("mondo.", ""): [] for disorder in disorders}

    for doc in coll.find(query):
        drug = doc["sourceDomainId"].replace("drugbank.", "")
        disorder = doc["targetDomainId"].replace("mondo.", "")
        results[disorder].append(drug)

    return results


@router.get("/get_drugs_targetting_proteins")
@check_api_key_decorator
def get_drugs_targetting_proteins(proteins: list[str] = PROTEIN_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    proteins = [f"uniprot.{i}" if not i.startswith("uniprot.") else i for i in proteins]

    coll = MongoInstance.DB()["drug_has_target"]
    query = {"targetDomainId": {"$in": proteins}}

    # NOTE: This ensures that all query disorders appear in the results
    results: dict[str, list[str]] = {protein.replace("uniprot.", ""): [] for protein in proteins}

    for doc in coll.find(query):
        drug = doc["sourceDomainId"].replace("drugbank.", "")
        protein = doc["targetDomainId"].replace("uniprot.", "")
        results[protein].append(drug)

    return results


@router.get("/get_drugs_targetting_gene_products")
@check_api_key_decorator
def get_drugs_targetting_gene_products(genes: list[str] = GENE_QUERY, x_api_key: str = _API_KEY_HEADER_ARG):
    gene_products = get_encoded_proteins(genes)
    all_proteins = list(_chain(*gene_products.values()))

    drugs_targetting_proteins = get_drugs_targetting_proteins(all_proteins)

    # NOTE: This ensures that all query disorders appear in the results
    results: dict[str, list[str]] = {gene.replace("entrez.", ""): [] for gene in genes}

    for gene, encoded_proteins in gene_products.items():
        for protein in encoded_proteins:
            drugs_targetting_protein = drugs_targetting_proteins.get(protein, [])
            results[gene] += drugs_targetting_protein

    return results
