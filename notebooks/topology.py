import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from sklearn.datasets import make_blobs
import util
from scipy.stats import lognorm
import sys
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import random

path_FIT = "datasets/FIT/coords/FIT0_calc_coords.csv"
path_RIPE_ATLAS = "datasets/RIPEAtlas/19062023/RIPEAtlas_coords.csv"
path_KING = "datasets/vivaldi_king.txt"
path_PLANETLAB = "datasets/planetlab.txt"


def get_lognorm_hist():
    mode = 1
    stddev = 50
    sigma, scale = util.lognorm_params(mode, stddev)
    sample = lognorm.rvs(sigma, 0, scale, size=10000).astype(int)
    H, bins = np.histogram(sample, bins=100, range=(0, 100), density=True)
    return H, bins


def get_lognorm_samples(min, max, num_samples, mu=0.8, sigma=1.5):
    lognorm_samples = lognorm.rvs(s=sigma, loc=0, scale=np.exp(mu), size=num_samples)
    # Clip the generated samples to the specified range
    lognorm_samples = np.clip(lognorm_samples, min, max)
    lognorm_samples = np.round(lognorm_samples).astype(int)
    return lognorm_samples


def add_capacity_columns(df, H, max_capacity, c_capacity, size):
    slot_columns = []
    for i in range(len(H), 0, -1):
        if (i % 10 == 0) or (i == 5) or (i == 1):
            # probabilites
            p = np.array(H[i - 1:len(H)])
            p /= p.sum()  # normalize
            pop = np.arange(i - 1, len(H))

            slot_list = np.random.choice(pop, size - 1, p=p, replace=True)
            slot_list = np.insert(slot_list, 0, 0)

            col = "capacity_" + str(i)
            df[col] = pd.Series(slot_list, dtype="int")
            df["capacity_" + str(i)] = df[col] / df[col].sum() * max_capacity

            df[col] = np.ceil(df[col]).astype("int")
            df.at[0, col] = c_capacity
            slot_columns.append(col)
    return df, slot_columns


def coords_ripe_atlas(path=path_RIPE_ATLAS):
    df_atlas = pd.read_csv(path, sep=",", header=None, names=["x", "y"])
    return df_atlas


def coords_fit(path=path_FIT):
    df = pd.read_csv(path, header=None, names=["x", "y"])
    return df


def coords_KING(path=path_KING):
    df_king = pd.read_csv(path, sep=" ", header=None, names=["name", "x", "y"])
    return df_king[["x", "y"]]


def coords_PLANETLAB(path=path_PLANETLAB):
    df_plb = pd.read_csv(path, sep=" ", header=None, names=["name", "x", "y"])
    return df_plb[["x", "y"]]


def coords_sim(size, centers=40, x_dim_range=(0, 100), y_dim_range=(-50, 50), with_latency=False,
               seed=None, c_coords=None):
    if seed:
        np.random.seed(seed)

    device_number = size + 1  # first node is the coordinator

    # blobs with varied variances
    stds = np.random.uniform(low=0.5, high=5.3, size=(centers,))
    if seed:
        coords, y = make_blobs(n_samples=device_number, centers=centers, n_features=2, shuffle=True,
                               cluster_std=stds, random_state=seed,
                               center_box=((x_dim_range[0], y_dim_range[0]), (x_dim_range[1], y_dim_range[1])))
    else:
        coords, y = make_blobs(n_samples=device_number, centers=centers, n_features=2, shuffle=True,
                               cluster_std=stds,
                               center_box=((x_dim_range[0], y_dim_range[0]), (x_dim_range[1], y_dim_range[1])))

    df = pd.DataFrame(coords, columns=["x", "y"])
    if with_latency:
        if c_coords is None:
            c_coords = df.iloc[0].to_numpy()
        else:
            df.loc[0, ["x", "y"]] = c_coords
        df['latency'] = list(zip(df.x, df.y))
        df['latency'] = df['latency'].apply(lambda x: np.linalg.norm(x - c_coords))

    return df


def get_coords_dict():
    out = {
        "planetlab": coords_PLANETLAB(),
        "king": coords_KING(),
        "fit": coords_fit(),
        "atlas": coords_ripe_atlas()
    }
    return out


def add_knn_labels(df, no_cluster):
    ch_indices = random.sample(range(1, df.shape[0]), no_cluster)
    ch_coords = np.array(df.iloc[ch_indices][['x', 'y']].apply(tuple, axis=1))

    # Build the k-d tree for the centroids
    kdtree = cKDTree(np.vstack(ch_coords))
    # Query the k-d tree to find the closest centroid for each node
    closest_centroids_indices = kdtree.query(df[["x", "y"]].to_numpy(), k=1)[1]
    df["cluster"] = closest_centroids_indices
    df.loc[0, "cluster"] = -1
    return df


def add_kmeans_labels(df, opt_k=None, kmin=2, kmax=30, kseed=20):
    coords = df[["x", "y"]]
    sil = []

    if not opt_k:
        # dissimilarity would not be defined for a single cluster, thus, minimum number of clusters should be 2
        for k in range(kmin, kmax + 1):
            # if k % 5 == 0:
            #    print(k)
            kmeans = KMeans(n_clusters=k, n_init='auto', random_state=kseed).fit(coords)
            labels = kmeans.labels_
            sil.append(silhouette_score(coords, labels, metric='euclidean'))

        opt_k = np.argmax(sil)
        opt_k = kmin + opt_k
        print("Optimal k is", opt_k)

    cluster_alg = KMeans(n_clusters=opt_k, n_init='auto').fit(coords)
    labels = cluster_alg.labels_
    centroids = cluster_alg.cluster_centers_

    df["cluster"] = labels
    df.loc[0, "cluster"] = -1
    return df, centroids, opt_k, sil


def setup_topology(df, H, max_resources, c_capacity=50, weights=(1, 1), dist="lognorm"):
    df = df.copy()
    coords = df[["x", "y"]].to_numpy()
    c_coords = coords[0]

    df['latency'] = list(zip(df.x, df.y))
    df['latency'] = df['latency'].apply(lambda x: np.linalg.norm(x - c_coords))

    type_list = ["worker" for x in range(len(coords) - 1)]
    type_list.insert(0, "coordinator")
    df["type"] = pd.Series(type_list, dtype="category")

    base_col = "base"
    df[base_col] = sys.maxsize

    if dist == "lognorm":
        df["weight"] = get_lognorm_samples(weights[0], weights[1], len(coords))
    else:
        df["weight"] = np.random.randint(weights[0], weights[1] + 1, len(coords))
    df.at[0, "weight"] = 0

    df, slot_columns = add_capacity_columns(df, H, max_resources, c_capacity, len(coords))
    return df, c_coords, base_col, slot_columns


def create_topologies_from_dict(topology_dict, H, max_resources, c_capacity=50, weights=(1, 1), dist="lognorm",
                                with_clustering=True, kmin=2, kmax=30, kseed=20):
    out = {}
    for k, df in topology_dict.items():
        print("Creating df for", k)
        df, c_coords, base_col, slot_columns = setup_topology(df, H, max_resources, c_capacity=c_capacity,
                                                              weights=weights, dist=dist)
        out[k] = df, c_coords, base_col, slot_columns
        if with_clustering:
            df, centroids, opt_k, sil = add_kmeans_labels(df, kmin=kmin, kmax=kmax, kseed=kseed)
            out[k] = df, c_coords, base_col, slot_columns, centroids, opt_k, sil

    print("Done")
    return out
