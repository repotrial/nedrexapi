# NeDRexAPI - Scripts
This directory contains scripts required to run certain tasks in the API, including two submodules (`MultiSteinerBackend` and `nedrex_validation`). As the `nedrex_validation` library was initially configured to run with the live API, a number of changes are required:

- Any instances of [https://api.nedrex.net](https://api.nedrex.net) need to be replaced with the URL for the current API.
- Routes related to graph building (e.g., /graph_builder) now have an additional level before them (e.g., /graph_builder -> /graph/graph_builder).
- Instances of the `protein_encoded_by` type need to be replaced with the new `protein_encoded_by_gene` type.