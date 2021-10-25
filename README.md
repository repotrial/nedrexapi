# NeDRexAPI
This repository will contain the code related to running the NeDRexAPI, originally based on the repotrial/repodb_v2 repository.

Unlike the repotrial/repodb_v2 repository, this contains only the code related to running the NeDRex API.

##### Configuration notes
- `matplotlib` is used by BiCoN, but isn't configured to be ran in 'headless' mode. To do this, following advice [here](https://stackoverflow.com/questions/37604289/tkinter-tclerror-no-display-name-and-no-display-environment-variable), the `~/.config/matplotlib/matplotlibrc` file with modified.
