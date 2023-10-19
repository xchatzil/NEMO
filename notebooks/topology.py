import pandas as pd
import numpy as np
import numpy.random as rd
from sklearn.datasets import make_blobs
import util
from scipy.stats import lognorm
import sys


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


def setup_topology(H, max_resources, c_capacity=50, centers=40, x_dim_range=(0, 100), y_dim_range=(-50, 50),
                   size=1000, seed=4, weights=(1, 1), dist="lognorm"):
    np.random.seed(seed)
    device_number = size + 1  # first node is the coordinator
    types = ["coordinator", "worker"]
    types_dist = [types[1]]

    type_list = [types_dist[rd.randint(0, len(types_dist))] for x in range(device_number - 1)]
    type_list.insert(0, types[0])

    # blobs with varied variances
    stds = np.random.uniform(low=0.5, high=5.3, size=(centers,))
    coords, y = make_blobs(n_samples=device_number, centers=centers, n_features=2, shuffle=True,
                           cluster_std=stds,
                           center_box=((x_dim_range[0], y_dim_range[0]), (x_dim_range[1], y_dim_range[1])),
                           random_state=31)
    c_coords = coords[0]

    df = pd.DataFrame(coords, columns=["x", "y"])
    df['latency'] = list(zip(df.x, df.y))
    df['latency'] = df['latency'].apply(lambda x: np.linalg.norm(x - c_coords))
    df['type'] = pd.Series(type_list, dtype="category")
    base_col = "base"
    df[base_col] = sys.maxsize

    if dist == "lognorm":
        df["weight"] = get_lognorm_samples(weights[0], weights[1], size+1)
    else:
        df["weight"] = np.random.randint(weights[0], weights[1] + 1, size + 1)
    df.at[0, "weight"] = 0

    sums = []
    slot_columns = []
    for i in range(len(H), 0, -1):
        if (i % 10 == 0) or (i == 5) or (i == 1):
            # probabilites
            p = np.array(H[i - 1:len(H)])
            p /= p.sum()  # normalize
            pop = np.arange(i - 1, len(H))

            slot_list = np.random.choice(pop, device_number - 1, p=p, replace=True)
            slot_list = np.insert(slot_list, 0, 0)

            col = "capacity_" + str(i)
            df[col] = pd.Series(slot_list, dtype="int")
            df["capacity_" + str(i)] = df[col] / df[col].sum() * max_resources

            df[col] = np.ceil(df[col]).astype("int")
            df.at[0, col] = c_capacity
            sums.append((df[col].sum()))
            slot_columns.append(col)

    return df, coords, c_coords, base_col, slot_columns, sums
