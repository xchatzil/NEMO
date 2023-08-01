import numpy as np
import sys
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.transforms import Bbox

lcl = "black"
cmarker = "p"
ccolor = lcl
coordinator_label = Line2D([], [], color=lcl, marker=cmarker, linestyle='None', label='coordinator')
worker_label = Line2D([], [], color="grey", marker='o', linestyle='None', label='worker', markersize=4)
reassigned_label = Line2D([0, 1], [0, 1], linestyle='--', color=lcl, label='reassigned')
centroid_label = Line2D([], [], color="grey", marker='o', linestyle='None', label='centroid')
aggp_label = Line2D([], [], color=lcl, marker='x', linestyle='None', label='aggregation')


def knn(root_coords, search_indexes, elements):
    nn_idx = np.nan
    nn_dist = sys.maxsize
    for search_idx in search_indexes:
        end_coords = elements[search_idx]
        distance = np.linalg.norm(root_coords - end_coords)
        if distance < nn_dist:
            nn_dist = distance
            nn_idx = search_idx
    return nn_idx, nn_dist


def evaluate(df_route, coords):
    device_number = df_route.shape[0]

    # first node is the coordinator
    latency_hist = np.zeros(device_number)
    received_packets_hist = np.zeros(device_number)

    for i in range(1, device_number):
        # calculate euclidean distance which corresponds to the cost space (latency)
        dist = 0
        lat_route = df_route.at[i, "route"]
        start = coords[i]
        received_packets_hist[lat_route[0]] += 1
        for j in range(0, len(lat_route)):
            end = coords[lat_route[j]]
            dist = dist + np.linalg.norm(start - end)
            start = end
        latency_hist[i] = dist

    statistics = {"latency_distribution": latency_hist,
                  "received_packets": received_packets_hist}
    df_stats = pd.DataFrame(statistics)
    return df_stats


def replace(list, old_elem, new_elem):
    for pos, val in enumerate(list):
        if val == old_elem:
            list[pos] = new_elem


def lognorm_params(mode, stddev):
    """
    Given the mode and std. dev. of the log-normal distribution, this function
    returns the shape and scale parameters for scipy's parameterization of the
    distribution.
    """
    p = np.poly1d([1, -1, 0, 0, -(stddev / mode) ** 2])
    r = p.roots
    sol = r[(r.imag == 0) & (r.real > 0)].real
    shape = np.sqrt(np.log(sol))
    scale = mode * sol
    return shape, scale


def lighten_color(color, amount=0.5):
    """
    Lightens the given color by multiplying (1-luminosity) by the given amount.
    Input can be matplotlib color string, hex string, or RGB tuple.

    Examples:
    >> lighten_color('g', 0.3)
    >> lighten_color('#F034A3', 0.6)
    >> lighten_color((.3,.55,.1), 0.5)
    """
    import matplotlib.colors as mc
    import colorsys
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])


def get_diff(arr1, arr2):
    if len(arr1) != len(arr2):
        raise Exception("Lengths of arrays are not equivalent")
    output = []
    for i in range(0, len(arr1)):
        if not np.array_equiv(arr1[i], arr2[i]):
            output.append(i)
    return output


def plot(ax, paths, agg_points, c_coords, centroid_coords, coords, colors, labels, changed_labels=None, lval=0.2,
         leg_size=8, print_centroids=True):
    no_clusters = len(np.unique(labels))
    for cl_label in range(no_clusters):
        line_style = "-"
        if changed_labels and (cl_label in changed_labels):
            line_style = "--"

        # print connections with centroids
        if print_centroids:
            point1 = centroid_coords[cl_label]
            ax.scatter(point1[0], point1[1], s=50, color=colors[cl_label], zorder=4, label="centroid")
            for agg_point_idx in agg_points[cl_label]:
                point2 = coords[agg_point_idx]
                x_values = [point1[0], point2[0]]
                y_values = [point1[1], point2[1]]
                ax.plot(x_values, y_values, line_style, zorder=2, color=lighten_color(colors[cl_label], lval + 0.2))

        for agg_point_idx in agg_points[cl_label]:
            point2 = coords[agg_point_idx]
            ax.scatter(point2[0], point2[1], s=50, color=colors[cl_label], zorder=3, marker="x", label="agg. point")

        # print connections of agg points
        for p1_idx, p2_idx in paths[cl_label]:
            point1 = coords[p1_idx]
            point2 = coords[p2_idx]
            x_values = [point1[0], point2[0]]
            y_values = [point1[1], point2[1]]
            ax.plot(x_values, y_values, line_style, zorder=2, color=lighten_color(colors[cl_label], lval + 0.2))

    ax.scatter(coords[0, 0], coords[0, 1], s=100, color=ccolor, marker=cmarker, zorder=10)
    # in case all coords shall be plotted
    ax.scatter(coords[:, 0], coords[:, 1], s=10, color=[lighten_color(x, lval) for x in colors[labels]], zorder=-1)

    ax.set_xlabel('$network$ $coordinate_1$')
    ax.set_ylabel('$network$ $coordinate_2$')

    if changed_labels:
        ax.legend(handles=[coordinator_label, worker_label, centroid_label, aggp_label, reassigned_label],
                  loc="upper left", bbox_to_anchor=(0, 1), fontsize=leg_size)
    else:
        ax.legend(handles=[coordinator_label, worker_label, centroid_label, aggp_label], loc="upper left",
                  bbox_to_anchor=(0, 1),
                  prop={'size': leg_size})


def full_extent(ax, pad=0.0):
    """Get the full extent of an axes, including axes labels, tick labels, and
    titles."""
    # For text objects, we need to draw the figure first, otherwise the extents
    # are undefined.
    ax.figure.canvas.draw()
    items = ax.get_xticklabels() + ax.get_yticklabels()
    items += [ax, ax.title, ax.xaxis.label, ax.yaxis.label]
    items += [ax, ax.title]
    bbox = Bbox.union([item.get_window_extent() for item in items])

    return bbox.expanded(1.0 + pad, 1.0 + pad)
