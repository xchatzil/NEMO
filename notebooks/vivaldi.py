import numpy as np
import pandas as pd


def vivaldi(D_change, dim, N, K):
    # parameters
    delta = 0.25
    ce = 0.25
    iteration = 2000
    coord = np.zeros((N, dim))
    neighbor = np.zeros((N, K), dtype=int)
    neighbor_count = np.zeros(N, dtype=int)
    fperror = []
    error = np.ones(N)

    for i in range(N):
        coord[i, :] = np.zeros(dim)
        tmp = np.random.permutation(N)
        point = 0
        for j in range(N):
            if D_change[i, j] >= 0 and i != j:
                neighbor_count[i] += 1
        if neighbor_count[i] > K:
            neighbor_count[i] = K
        j = 0
        for ii in range(N):
            if D_change[i, tmp[ii]] >= 0 and i != tmp[ii]:
                neighbor[i, j] = tmp[ii]
                j += 1
                if j >= neighbor_count[i]:
                    break

    delta_temp = delta
    predicted_matrix = np.zeros((N, N), dtype=np.float32)

    for j in range(iteration):
        for i in range(N):
            if neighbor_count[i] == 0:
                continue
            ID_neigh = neighbor[i, np.random.randint(0, neighbor_count[i])]
            temp_delta = delta

            if D_change[ID_neigh, i] <= 0 or D_change[i, ID_neigh] <= 0:
                continue

            w = error[i] / (error[i] + error[ID_neigh])
            e_s = np.abs(np.linalg.norm(coord[i, :] - coord[ID_neigh, :]) - D_change[i, ID_neigh]) / D_change[
                i, ID_neigh]
            error[i] = e_s * ce * w + error[i] * (1 - ce * w)
            delta = delta_temp * w

            Force = delta * (D_change[i, ID_neigh] - np.linalg.norm(coord[i, :] - coord[ID_neigh, :])) * (
                    coord[i, :] - coord[ID_neigh, :]) / np.linalg.norm(coord[i, :] - coord[ID_neigh, :])
            coord[i, :] += Force
    return coord


def compute_coordinates(L, dim=2, n_iterations=1000, t=1):
    """Compute more accurate Vivaldi coordinates given a latency matrix and initial coordinates.

    Args:
        L (pandas.DataFrame): Latency matrix.
        x (pandas.DataFrame): Initial coordinates.
        n_iterations (int): Number of iterations to perform.
        t (float): Step size in the direction of the force.

    Returns:
        pandas.DataFrame: More accurate Vivaldi coordinates.
    """
    x = np.random.rand(len(L), dim)
    for _ in range(n_iterations):
        F = np.zeros(dim)
        for i in range(x.shape[0]):
            for j in range(x.shape[0]):
                if i == j:
                    continue
                v = x[i] - x[j]
                e = L[i, j] - np.linalg.norm(v)
                u = v / np.linalg.norm(v)
                F = F + e * u
        x = x + t * F
    return pd.DataFrame(x, columns=['x', 'y'])


def compute_vivaldi_coords(rtt_dict, alpha=1.0, num_iterations=100):
    # Get the list of unique nodes from the keys of the dictionary
    nodes = list(set(sum(rtt_dict.keys(), ())))
    n = len(nodes)

    # Initialize the coordinates to random values
    coords = np.random.rand(n, 2)

    # Set the reference coordinate to the center of the coordinate space
    ref_coord = np.array([50, 50])

    # Define the maximum round trip time
    max_rtt = max(rtt_dict.values())

    # Initialize the RMS error list
    rms_error = []

    # Iterate until convergence
    for i in range(num_iterations):
        for (u, v), rtt in rtt_dict.items():
            # Get the indices of the nodes in the coordinate array
            u_idx = nodes.index(u)
            v_idx = nodes.index(v)

            # Compute the distance between the coordinates of the nodes
            dist = np.linalg.norm(coords[u_idx] - coords[v_idx])

            # Compute the error in the round trip time prediction
            error = alpha * (dist - rtt) / max_rtt

            # Update the coordinates of the nodes using the error and reference coordinate
            coords[u_idx] -= error * (coords[u_idx] - ref_coord)
            coords[v_idx] += error * (coords[v_idx] - ref_coord)

        # Normalize the coordinates to the unit square
        coords -= coords.min(axis=0, initial=0)
        coords /= coords.max(axis=0, initial=0)

        # Compute the error vector for each pair of nodes
        errors = np.array(
            [[(coords[nodes.index(n1)] - coords[nodes.index(n2)]) - rtt_dict[(n1, n2)] for n1, n2 in rtt_dict.keys()]])
        # Compute the RMS error for this iteration and append to the list
        rms_error.append(np.sqrt(np.mean(errors ** 2)))

    return coords.tolist(), rms_error


def compute_vivaldi_coordinates_with_error(node_rtt, alpha=1.0, num_iterations=100):
    # Extract the list of nodes from the input dictionary
    nodes = list(set(sum(node_rtt.keys(), ())))

    # Initialize the coordinates randomly
    coords = np.random.rand(len(nodes), 2)

    # Initialize the RMS error list
    rms_error = []

    # Loop over the specified number of iterations
    for i in range(num_iterations):
        # Compute the error vector for each pair of nodes
        errors = np.array(
            [[(coords[nodes.index(n1)] - coords[nodes.index(n2)]) - node_rtt[(n1, n2)] for n1, n2 in node_rtt.keys()]])

        # Compute the RMS error for this iteration and append to the list
        rms_error.append(np.sqrt(np.mean(errors ** 2)))

        # Compute the gradient for each node
        gradient = np.zeros_like(coords)
        for j, node in enumerate(nodes):
            for k, other_node in enumerate(nodes):
                if node != other_node:
                    # Compute the gradient contribution for this pair of nodes
                    diff = coords[j] - coords[k]
                    dist = np.linalg.norm(diff)
                    if dist > 0:
                        gradient[j] += ((diff / dist) * (dist - node_rtt.get((node, other_node), 0)))

        # Update the coordinates using the gradient and the learning rate alpha
        coords -= alpha * gradient

    # Return the final coordinates
    return dict(zip(nodes, coords)), rms_error
