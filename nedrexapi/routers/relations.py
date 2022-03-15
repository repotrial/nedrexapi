from collections import defaultdict as _defaultdict
from itertools import chain as _chain

from fastapi import APIRouter as _APIRouter, Query as _Query
from pydantic import BaseModel as _BaseModel, Field as _Field

from nedrexapi.db import MongoInstance

router = _APIRouter()


class DisorderSeededRequest(_BaseModel):
    disorders: list[str] = _Field(None, title="Disorders", description="Disorders to get relationships for")


class GeneSeededRequest(_BaseModel):
    genes: list[str] = _Field(None, title="Genes", description="Genes to get relationships for")


class ProteinSeededRequest(_BaseModel):
    proteins: list[str] = _Field(None, title="Proteins", description="Proteins to get relationships for")


@router.get("/get_encoded_proteins")
def get_encoded_proteins(
    genes: list[str] = _Query(
        None,
        title="Genes",
        description=(
            "Gene(s) to get relationships for. " "Multiple genes can be specified (e.g., gene=entrez.1&gene=entrez.2)"
        ),
        alias="gene",
    )
):
    """
    Given a set of seed genes, this route returns the proteins encoded by those genes as a hash map.
    """
    genes = [f"entrez.{i}" if not i.startswith("entrez") else i for i in genes]

    coll = MongoInstance.DB()["protein_encoded_by_gene"]
    query = {"targetDomainId": {"$in": genes}}

    results = _defaultdict(list)
    for doc in coll.find(query):
        gene = doc["targetDomainId"].replace("entrez.", "")
        protein = doc["sourceDomainId"].replace("uniprot.", "")
        results[gene].append(protein)

    return results


@router.get("/get_drugs_indicated_for_disorders")
def get_drugs_indicated_for_disorders(dr: DisorderSeededRequest):
    disorders = [f"mondo.{i}" if not i.startswith("mondo") else i for i in dr.disorders]

    coll = MongoInstance.DB()["drug_has_indication"]
    query = {"targetDomainId": {"$in": disorders}}

    results = _defaultdict(list)
    for doc in coll.find(query):
        drug = doc["sourceDomainId"].replace("drugbank.", "")
        disorder = doc["targetDomainId"].replace("mondo.", "")
        results[disorder].append(drug)

    return results


@router.get("/get_drugs_targetting_proteins")
def get_drugs_targetting_proteins(sr: ProteinSeededRequest):
    proteins = [f"uniprot.{i}" if not i.startswith("uniprot.") else i for i in sr.proteins]

    coll = MongoInstance.DB()["drug_has_target"]
    query = {"targetDomainId": {"$in": proteins}}

    results = _defaultdict(list)
    for doc in coll.find(query):
        drug = doc["sourceDomainId"].replace("drugbank.", "")
        protein = doc["targetDomainId"].replace("uniprot.", "")
        results[protein].append(drug)

    return results


@router.get("/get_drugs_targetting_gene_products")
def get_drugs_targetting_gene_products(sr: GeneSeededRequest):
    gene_products = get_encoded_proteins(sr)
    all_proteins = list(_chain(*gene_products.values()))

    protein_seed_request = ProteinSeededRequest(proteins=all_proteins)
    drugs_targetting_proteins = get_drugs_targetting_proteins(protein_seed_request)

    results: dict[str, list[str]] = _defaultdict(list)
    for gene, encoded_proteins in gene_products.items():
        for protein in encoded_proteins:
            drugs_targetting_protein = drugs_targetting_proteins.get(protein, [])
            results[gene] += drugs_targetting_protein

    return results
