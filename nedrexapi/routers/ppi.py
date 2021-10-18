from fastapi import APIRouter as _APIRouter, Query as _Query

from nedrexapi.db import MongoInstance

DEFAULT_QUERY = _Query(None)

router = _APIRouter()


@router.get("/ppi", summary="Get filtered PPIs")
def get_filtered_protein_protein_interactions(iid_evidence: list[str] = DEFAULT_QUERY):
    """
    Returns an array of protein protein interactions (PPIs), filtered according to the evidence types given in the
    `?iid_evidence` query parameter(s).
    A PPI is a JSON object with "memberOne" and "memberTwo" attributes containing the primary domain IDs of the
    interacting proteins.
    Additional information, such as source databases and experimental methods are contained with each entry.
    The options available for `iid_evidence` are `["pred", "ortho", "exp"]`, reflecting predicted PPIs, orthologous
    PPIs, and experimentally detected PPIs respectively.
    Note that there are many PPIs in the database, and so this route can take a while to respond.
    """
    if not iid_evidence:
        return []

    coll_name = "protein_interacts_with_protein"

    query = {"evidenceTypes": {"$in": iid_evidence}}
    results = [{k: v for k, v in doc.items() if k != "_id"} for doc in MongoInstance.DB()[coll_name].find(query)]
    return results
