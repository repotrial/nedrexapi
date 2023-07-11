import time
from urllib.request import urlretrieve
import requests  # type: ignore
import os
import graph_tool as gt
import networkx as nx


apiNetwork_path = "/srv/nedrex/nedrex_v2/nedrex_files/nedrex_api/static/"
os.chdir(apiNetwork_path)
# get the network containing protein-protein and protein-drug interactions with proper parameters via API
base_url = "https://api.nedrex.net/licensed"
api_key_url= f"{base_url}/admin/api_key/generate"

api_key_payload = {"accept_eula":True}
api_key = requests.post(api_key_url, json=api_key_payload)
if '"' in api_key.text:
    api_key = api_key.text.split('"')[1]

headers = {"x-api-key": api_key}

submit_url = f"{base_url}/graph/builder"

data = {
    "nodes": [],
    "edges": ["protein_interacts_with_protein", "drug_has_target"],
    "drug_groups": [
        "approved",
        "experimental",
        "investigational",
        "nutraceutical",
        "vet_approved",
        "withdrawn",
        "illicit",
    ],
    "concise": True,
}

print("Submitting request")
gbuild = requests.post(submit_url, json=data, headers=headers)
print(gbuild.status_code, gbuild.text)
print(f"UID for job: {gbuild.json()}")
uid = gbuild.json()

while True:
    progress = requests.get(f"{base_url}/graph/details/{uid}", headers=headers)
    built = progress.json()["status"] == "completed"
    if built:
        break
    print("Waiting for build to complete, sleeping for 10 seconds")
    time.sleep(10)

fname = "temp-PPDr"
urlretrieve(f"{base_url}/graph/download/{uid}/{fname}.graphml", f"{fname}.graphml")


G = nx.read_graphml(f"{apiNetwork_path}{fname}.graphml")
G_und = G.to_undirected()

node_list = set(G_und.nodes)
nodeAttr_list = {"geneName", "taxid", "domainIds", "synonyms", "indication", "displayName"}
for n in node_list:
    for attr in nodeAttr_list:
        if attr in G_und.nodes[n].keys():
            del G_und.nodes[n][attr]

edge_list = set(G_und.edges)
edgeAttr_list = {"memberOne", "memberTwo", "reversible", "sourceDomainId", "targetDomainId"}
for e in edge_list:
    for attr in edgeAttr_list:
        if attr in G_und.edges[e].keys():
            del G_und.edges[e][attr]

network_name = "PPDr-for-ranking.graphml"
nx.write_graphml(G_und, f"{apiNetwork_path}{network_name}")

gg = gt.load_graph("PPDr-for-ranking.graphml")
gg.save("PPDr-for-ranking.gt")

# Remove temporary graphs
os.remove(f"{apiNetwork_path}temp-PPDr.graphml")
