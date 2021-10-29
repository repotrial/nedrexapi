#!/usr/bin/env python
import json
from pathlib import Path

from bicon import data_preprocessing
from bicon import BiCoN
from bicon import results_analysis
import click
import numpy as np


@click.command()
@click.option("--expression", type=click.Path(exists=True), required=True)
@click.option("--network", type=click.Path(exists=True), required=True)
@click.option("--lg_min", default=10)
@click.option("--lg_max", default=15)
@click.option("--outdir", type=click.Path(exists=True), required=True)
def run(expression, network, lg_min, lg_max, outdir):
    path_expr, path_net = str(expression), str(network)

    print("Preprocessing")
    GE, G, labels, _ = data_preprocessing(path_expr, path_net)

    print("Running BiCoN")
    model = BiCoN(GE, G, lg_min, lg_max)
    print("Getting solution and scores")
    solution, scores = model.run_search(logging=True, n_proc=8)
    print("Running result analysis")
    results = results_analysis(solution, labels)

    print("Saving output")
    results.save(output=Path(outdir) / "results.csv")
    print("Saving network visualization")
    results.show_networks(GE, G, output=Path(outdir) / "network.png")
    print("Saving clustermap")
    results.show_clustermap(GE, G, output=Path(outdir) / "clustermap.png")

    # Storing data
    all_genes_entr = results.genes1 + results.genes2
    all_genes = results.solution[0][0] + results.solution[0][1]

    GE_small = GE[results.solution[1][0] + results.solution[1][1]].loc[all_genes]
    GE_small.index = all_genes_entr
    GE_small.columns = results.patients1 + results.patients2

    p1g1_mean = np.mean(GE_small[results.patients1].loc[results.genes1], axis=1)
    p2g1_mean = np.mean(GE_small[results.patients2].loc[results.genes1], axis=1).values
    means1 = list(p1g1_mean - p2g1_mean)

    p1g2_mean = np.mean(GE_small[results.patients1].loc[results.genes2], axis=1)
    p2g2_mean = np.mean(GE_small[results.patients2].loc[results.genes2], axis=1).values
    means2 = list(p1g2_mean - p2g2_mean)

    result = {}
    result["genes1"] = [{"gene": gene, "mean diff expression": m} for gene, m in zip(results.genes1, means1)]
    result["genes2"] = [{"gene": gene, "mean diff expression": m} for gene, m in zip(results.genes2, means2)]

    json_out = Path(outdir) / "results.json"

    with json_out.open("w") as f:
        json.dump(result, f)

    print("Done!")


if __name__ == "__main__":
    run()
