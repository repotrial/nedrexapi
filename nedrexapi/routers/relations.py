from itertools import chain as _chain

from fastapi import APIRouter as _APIRouter
from fastapi import Query as _Query

from nedrexapi.common import _API_KEY_HEADER_ARG, check_api_key_decorator
from nedrexapi.db import MongoInstance
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

router = _APIRouter()

class NodeListRequest(_BaseModel):
    nodes: list[str] = _Field(None, title="Primary domain IDs of nodes",
                              description="Primary domain IDs of the nodes the attributes are requested for")
    class Config:
        extra = "forbid"


_DEFAULT_NODE_REQUEST = NodeListRequest()

def make_node_list_request(items: list[str]) -> NodeListRequest:
    request = NodeListRequest()
    request.nodes = items
    return request


@router.post("/get_encoded_proteins")
@check_api_key_decorator
def get_encoded_proteins(genes: NodeListRequest = _DEFAULT_NODE_REQUEST, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Given a set of seed genes, this route returns the proteins encoded by those genes as a hash map.
    """
    genes = [f"entrez.{i}" if not i.startswith("entrez") else i for i in genes.nodes]

    coll = MongoInstance.DB()["protein_encoded_by_gene"]
    query = {"targetDomainId": {"$in": genes}}

    # NOTE: This ensures that all query disorders appear in the results
    results: dict[str, list[str]] = {gene.replace("entrez.", ""): [] for gene in genes}

    for doc in coll.find(query):
        gene = doc["targetDomainId"].replace("entrez.", "")
        protein = doc["sourceDomainId"].replace("uniprot.", "")
        results[gene].append(protein)

    return results


@router.post("/get_drugs_indicated_for_disorders")
@check_api_key_decorator
def get_drugs_indicated_for_disorders(disorders: NodeListRequest = _DEFAULT_NODE_REQUEST,
                                      x_api_key: str = _API_KEY_HEADER_ARG):
    disorders = [f"mondo.{i}" if not i.startswith("mondo") else i for i in disorders.nodes]

    coll = MongoInstance.DB()["drug_has_indication"]
    query = {"targetDomainId": {"$in": disorders}}

    # NOTE: This ensures that all query disorders appear in the results
    results: dict[str, list[str]] = {disorder.replace("mondo.", ""): [] for disorder in disorders}

    for doc in coll.find(query):
        drug = doc["sourceDomainId"].replace("drugbank.", "")
        disorder = doc["targetDomainId"].replace("mondo.", "")
        results[disorder].append(drug)

    return results


@router.post("/get_drugs_targeting_proteins")
@check_api_key_decorator
def get_drugs_targeting_proteins(proteins: NodeListRequest = _DEFAULT_NODE_REQUEST, x_api_key: str = _API_KEY_HEADER_ARG):
    proteins = [f"uniprot.{i}" if not i.startswith("uniprot.") else i for i in proteins.nodes]

    coll = MongoInstance.DB()["drug_has_target"]
    query = {"targetDomainId": {"$in": proteins}}

    # NOTE: This ensures that all query disorders appear in the results
    results: dict[str, list[str]] = {protein.replace("uniprot.", ""): [] for protein in proteins}

    for doc in coll.find(query):
        drug = doc["sourceDomainId"].replace("drugbank.", "")
        protein = doc["targetDomainId"].replace("uniprot.", "")
        results[protein].append(drug)

    return results


@router.post("/get_drugs_targeting_gene_products")
@check_api_key_decorator
def get_drugs_targeting_gene_products(genes: NodeListRequest = _DEFAULT_NODE_REQUEST, x_api_key: str = _API_KEY_HEADER_ARG):
    gene_products = get_encoded_proteins(genes)
    all_proteins = list(_chain(*gene_products.values()))

    drugs_targeting_proteins = get_drugs_targeting_proteins(make_node_list_request(all_proteins))

    # NOTE: This ensures that all query disorders appear in the results
    results: dict[str, list[str]] = {gene.replace("entrez.", ""): [] for gene in genes.nodes}

    for gene, encoded_proteins in gene_products.items():
        for protein in encoded_proteins:
            drugs_targeting_protein = drugs_targeting_proteins.get(protein, [])
            results[gene] += drugs_targeting_protein

    return results
