# NEMO
Code for **Efficient Placement of Decomposable Aggregation Functions for Stream Processing over Large Geo-Distributed Topologies**

The repository contains two projects:
1. notebooks: Jupyter notebooks of the local experiments and plots of all evaluation results
2. nebulastream: A fork of the NebulaStream master with the NEMO integration

## Python Ιncludes 
- Python implementations of the following approaches:
    - NEMO
    - LEACH
    -  MST
    -  Chain
    -  Optimal
- Jupyter notebooks with evaluations of the implemented approaches for the following topologies:
    - ASimulations.ipynb
        - Experiments on artificial topologies of different sizes
    - FitLab.ipynb
        - Experiments on the FIT IoT Lab
    - King.ipynb
        - Experiments on the King dataset
    - Planetlab.ipynb
        - Experiments on the PlanetLab dataset
    - RIPEAtlas.ipynb
        - Experiments on the RIPE Atlas dataset
- Jupyter notebooks with additional plots of conducted experiments:
    - NesDeploymentsEval: Contain the plots for the End-to-end performance evaluation of NEMO in NebulaStream
    - Heatmaps: Contain heatmaps about latency statistics of the implemented approach for the tested topologies
    - ScalabilityTests: Contain the scalability experiments and plots
 
## Python Dependencies 
All required dependencies are specified in notebooks/requirements.txt

## Datasets
All datasets of the topologies and results of our experiments can be found in notebooks/datasets

## NebulaStream
NEMO is currently part of the master branch of NebulaStream: https://github.com/nebulastream/nebulastream

We provide in this repository a fork of NebulaStream with our integration of NEMO. A locally runnable Integration test can be found in nebulastream/nes-core/tests/Integration/NemoIntegrationTest.cpp

For a detailed guide how to run and deploy NebulaStream, we refer to: https://docs.nebula.stream/
