# NEMO: Efficient Placement of Decomposable Aggregation Functions for Stream Processing over Large Geo-Distributed Topologies

NEMO is a heuristic approach for determining the replication factor and placement of decomposable aggregation functions (DAFs) in geo-distributed stream processing environments.
It targets osmotic and IoT settings with heterogeneous, resource-constrained nodes and prioritizes minimizing end-to-end latency while avoiding node overloading.

NEMO projects a topology into a Euclidean network coordinate space (NCS), identifies optimal virtual operator locations, and maps them to physical nodes through resource-aware replication and reassignment.
The approach scales to topologies with millions of nodes, supports efficient re-optimizations under topology changes, and can be extended to handle arbitrary link weights (NEMO+) and additional cost dimensions.

### Approach

NEMO operates in three phases:

1. **Grouping the cost space** — Nodes are clustered in the NCS to restrict the search space and maximize intra-cluster cohesion (e.g., via k-means).
2. **Virtual operator placement** — For each latency cluster, NEMO iteratively builds an aggregation tree using spring relaxation to determine virtual operator locations between upstream nodes and the sink.
3. **Re-assignment and replication** — Physical cluster heads are selected near virtual nodes based on available capacity, distributing load across underutilized nodes.

When sink capacity is insufficient, NEMO adds intermediate partial-window aggregation levels until the placement converges.
Re-optimizations for node additions and removals can be performed without recomputing the full placement.

### Repository Structure

This repository contains the Python simulation code and Jupyter notebooks used to evaluate NEMO in the VLDB 2024 paper.

| Component | Description |
|-----------|-------------|
| [notebooks](notebooks) | Jupyter notebooks for experiments, plots, and evaluation results across real-world and artificial topologies. |
| [notebooks/nemo.py](notebooks/nemo.py) | Core NEMO and NEMO+ placement solver (`NemoSolver`). |
| [notebooks/leach.py](notebooks/leach.py) | LEACH and LEACH-SF baseline implementations. |
| [notebooks/mst_prim.py](notebooks/mst_prim.py) | MST (Prim-based) baseline for tree aggregation. |
| [notebooks/tsp.py](notebooks/tsp.py) | Chain baseline using simulated annealing. |
| [notebooks/placement_model.py](notebooks/placement_model.py) | Optimal placement model (Cardellini et al.). |
| [notebooks/vivaldi.py](notebooks/vivaldi.py) | Vivaldi network coordinate system implementation. |
| [notebooks/topology.py](notebooks/topology.py) | Topology generation and preprocessing utilities. |
| [notebooks/util.py](notebooks/util.py) | Shared evaluation, plotting, and clustering utilities. |
| [notebooks/datasets](notebooks/datasets) | Latency measurements, NCS coordinates, and NebulaStream deployment results. |
| [notebooks/plots](notebooks/plots) | Generated figures from the paper evaluation. |

### Baselines

The simulation code includes Python implementations of the following aggregation and placement approaches:

- **NEMO** and **NEMO+** (arbitrary link weights)
- **LEACH** and **LEACH-SF**
- **MST** (minimum spanning tree)
- **Chain** (probabilistic chain aggregation)
- **Optimal** (ILP-based, Cardellini et al.)
- **Bottom-up** and **top-down** (NebulaStream heuristics)

### Notebooks

| Notebook | Description |
|----------|-------------|
| [ASimulations.ipynb](notebooks/ASimulations.ipynb) | Experiments on artificial topologies of varying sizes (up to 1M nodes). |
| [FitLab.ipynb](notebooks/FitLab.ipynb) | Evaluation on the FIT IoT Lab topology. |
| [RIPEAtlas.ipynb](notebooks/RIPEAtlas.ipynb) | Evaluation on the RIPE Atlas topology. |
| [PlanetLab.ipynb](notebooks/PlanetLab.ipynb) | Evaluation on the PlanetLab topology. |
| [King.ipynb](notebooks/King.ipynb) | Evaluation on the King DNS server topology. |
| [WeightSimulations.ipynb](notebooks/WeightSimulations.ipynb) | NEMO+ experiments with varying link weights. |
| [ScalabilityTests.ipynb](notebooks/ScalabilityTests.ipynb) | Full-optimization and re-optimization scalability experiments. |
| [Heatmaps.ipynb](notebooks/Heatmaps.ipynb) | Latency heatmaps across approaches and topologies. |
| [ChangingTopology.ipynb](notebooks/ChangingTopology.ipynb) | Robustness experiments under topology changes. |
| [NesDeploymentsEval_latency.ipynb](notebooks/NesDeploymentsEval_latency.ipynb) | End-to-end latency evaluation on a Raspberry Pi NebulaStream cluster. |
| [NesDeploymentsEval_perf.ipynb](notebooks/NesDeploymentsEval_perf.ipynb) | End-to-end communication cost evaluation on NebulaStream. |

Preprocessing and clustering notebooks (`*_preprocessing.ipynb`, `RIPEAtlas_clustering.ipynb`, `FuzzyClustering.ipynb`) prepare topology data and NCS coordinates for the main evaluation notebooks.

### Running Simulations

Install the required Python packages and run the notebooks from the `notebooks` directory:

```sh
pip install numpy pandas scipy scikit-learn matplotlib seaborn scikit-fuzzy jupyter
cd notebooks
jupyter notebook
```

### Datasets

Topology latency measurements, network coordinate files, and NebulaStream deployment results are available in [notebooks/datasets](notebooks/datasets):

- **FIT** — FIT IoT Lab RTT measurements
- **RIPEAtlas** — RIPE Atlas anchor measurements
- **planetlab.txt**, **PL_lat.csv**, **PL_coords.txt** — PlanetLab data
- **king_lat.txt**, **vivaldi_king.txt** — King DNS server topology
- **NES** — End-to-end deployment results

### End-to-End Experiments

The end-to-end experiments in the paper were conducted with [NebulaStream](https://github.com/nebulastream/nebulastream).

### Publication

This repository accompanies the following publication:

```BibTeX
@article{DBLP:journals/pvldb/ChatziliadisZEZM24,
  author       = {Xenofon Chatziliadis and
                  Eleni Tzirita Zacharatou and
                  Alphan Eracar and
                  Steffen Zeuch and
                  Volker Markl},
  title        = {Efficient Placement of Decomposable Aggregation Functions for Stream
                  Processing over Large Geo-Distributed Topologies},
  journal      = {Proc. {VLDB} Endow.},
  volume       = {17},
  number       = {6},
  pages        = {1501--1514},
  year         = {2024},
  url          = {https://www.vldb.org/pvldb/vol17/p1501-chatziliadis.pdf},
  doi          = {10.14778/3648160.3648186},
  biburl       = {https://dblp.org/rec/journals/pvldb/ChatziliadisZEZM24.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```

### Related Work

The following work is closely related to NEMO and influenced its design:

* [Pietzuch et al.](https://doi.org/10.1109/ICDE.2006.105): Network-Aware Operator Placement for Stream-Processing Systems (ICDE 2006).
* [Rizou et al.](https://doi.org/10.1109/ICCCN.2010.5560127): Solving the Multi-Operator Placement Problem in Large-Scale Operator Networks (ICCCN 2010).
* [Dabek et al.](https://doi.org/10.1145/1015467.1015471): Vivaldi: A decentralized network coordinate system (SIGCOMM 2004).
* [Zeuch et al.](https://www.cidrdb.org/cidr2020/papers/p7-zeuch-cidr20.pdf): The NebulaStream Platform for Data and Application Management in the Internet of Things (CIDR 2020).
