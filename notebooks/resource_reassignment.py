from util import replace, evaluate



def reassign_cluster_heads(df, slot_col):
    # print("Reassigning cluster heads for slot column ", slot_col)
    df["new_parent"] = df["parent"]
    df["new_route"] = df["route"]

    # calculate all parents that are overloaded and iterate through their according group
    load_counts = df["parent"].value_counts()
    load_dict = load_counts.to_dict()
    df["load"] = df.index.map(load_dict).fillna(0).astype("int")
    df["av_slots"] = df[slot_col] - df["load"]
    df_overloaded = df[["parent", "load", "av_slots"]][df["av_slots"] < 0]
    df_grouped = df[df["parent"].isin(df_overloaded.index)].groupby(["parent", "cluster"])
    new_paths_dict = {}
    agg_dict = {}

    for group, df_group in df_grouped:
        df_group = df.iloc[df_group[:].index][["av_slots", "latency"]].copy()
        old_parent_idx = group[0]
        cluster_no = group[1]
        cluster_path = []
        agg_points = []
        new_paths_dict[cluster_no] = cluster_path
        agg_dict[cluster_no] = agg_points

        parent_parent_idx = df.iloc[old_parent_idx]["parent"]
        total_required_slots = df_group.shape[0]

        # release at first the required slots at the parent
        df.at[old_parent_idx, "av_slots"] = df.iloc[old_parent_idx][slot_col]
        df_group["weighted"] = df_group["av_slots"] / (total_required_slots * df_group["latency"])

        # calc cumsum over available sorted slots
        df_sorted = df_group.sort_values(["weighted"], ascending=False)
        df_sorted["cumsum"] = df_sorted["av_slots"].cumsum()

        # print("Parent ", old_parent_idx, " is overloaded")
        parent_slots_av = df.iloc[old_parent_idx]["av_slots"]
        slot_index = (df_sorted["cumsum"].values.searchsorted(total_required_slots))
        new_parent_indexes = []
        for i in range(0, slot_index + 1):
            new_parent_idx = df_sorted.index[i]
            new_parent_indexes.append(new_parent_idx)

        # remove new and old cluster heads from the assignment
        all_cidxs = [ele for ele in df_group[:].index if
                     (ele not in new_parent_indexes) and (ele != old_parent_idx)]

        # first assign nodes to their new parent indexes, except the new parents and old parent
        assigned = 0
        for new_parent_idx in new_parent_indexes:
            if assigned <= len(all_cidxs):
                # get indexes of new children
                new_parent_slots = df.iloc[new_parent_idx]["av_slots"]
                cidxs = all_cidxs[assigned:assigned + new_parent_slots]
                # update parent
                df.loc[cidxs, "new_parent"] = new_parent_idx
                # print("Updating parent from ", old_parent_idx, " to ", new_parent_idx, " for indexes ", assigned,
                #      " to ", assigned + new_parent_slots)
                assigned = assigned + new_parent_slots
                # update routes
                for j in cidxs:
                    old_parent = df.at[j, "parent"]
                    route_list = df.at[j, "new_route"][:]
                    replace(route_list, old_parent, new_parent_idx)
                    df.at[j, "new_route"] = route_list

        # print("New parents for cluster ", cluster_no, " are: ", new_parent_indexes)

        if parent_slots_av >= len(new_parent_indexes):
            # if old parent has capacities for the new cluster heads everything is fine
            # print("Old main parent ", old_parent_idx, " has enough resources(", parent_slots_av,
            #      ") to remain head for new intermediates (", len(new_parent_indexes), ")")
            for parent in new_parent_indexes:
                cluster_path.append((parent, parent_parent_idx))
                agg_points.append(parent)
            continue
        else:
            # else assign the remaining points to the cluster head which has the remaining resources left (last element)
            new_parent_parent_idx = new_parent_indexes[-1]
            remaining_nodes = new_parent_indexes[:-1] + [old_parent_idx]
            print("Setting new parent ", new_parent_parent_idx, " for slots ", slot_col, " and remaining nodes ",
                  remaining_nodes)
            # update parent
            df.loc[remaining_nodes, "new_parent"] = new_parent_parent_idx
            # update routes
            for j in remaining_nodes:
                old_parent = df.at[j, "parent"]
                route_list = df.at[j, "new_route"][:]
                if j == old_parent_idx:
                    route_list.insert(0, new_parent_parent_idx)
                else:
                    replace(route_list, old_parent, new_parent_parent_idx)
                cluster_path.append((new_parent_parent_idx, j))
                agg_points.append(j)
                df.at[j, "new_route"] = route_list
            # update new-parent-parent parent
            df.loc[new_parent_parent_idx, "new_parent"] = parent_parent_idx
            # update new-parent-parent routes
            route_list = df.loc[new_parent_parent_idx, "new_route"][:]
            route_list.remove(old_parent_idx)
            df.at[new_parent_parent_idx, "new_route"] = route_list
    return df, new_paths_dict, agg_dict


def get_cluster_heads(elements, df, res_threshold, av_col_name="free_slots", parent_col_name="parent"):
    # Initialize the result dictionary with the optimal index
    ch_dict = {}
    current = 0
    while elements:
        min_idx, av_resources, nnr = elements[current]
        if av_resources <= res_threshold:
            elements.pop(0)
            continue

        max_idx, nna, required = elements[len(elements) - 1]
        if max_idx in ch_dict:
            break

        if av_resources >= required:
            # update values of the cluster head
            new_available = av_resources - required
            elements[0] = (min_idx, new_available, nnr)
            create_mapping(max_idx, min_idx, df, ch_dict, new_available, av_col_name, parent_col_name)
            elements.pop(len(elements) - 1)
        else:
            # not enough resources, split mapping
            remaining_required = required - av_resources
            elements[0] = (min_idx, 0, nnr)
            elements[len(elements) - 1] = max_idx, nna, remaining_required
            create_mapping(max_idx, min_idx, df, ch_dict, 0, av_col_name, parent_col_name)

    return df, ch_dict


def create_mapping(child_idx, parent_idx, df, ch_dict, new_parent_resources, av_column, parent_column):
    # create mapping
    if parent_idx in ch_dict:
        ch_dict[parent_idx].append(child_idx)
    else:
        ch_dict[parent_idx] = [child_idx]

    # update parent resources
    df.at[parent_idx, av_column] = new_parent_resources
    # update routes of the added node
    df.at[child_idx, parent_column] = df.at[child_idx, parent_column] + [parent_idx]


def distribute_resources_and_evaluate(slot_columns, df, coords):
    eval_matrix_slots = {}
    df_stats = evaluate(df, coords)
    eval_matrix_slots["base"] = df_stats.copy()
    col_paths = {}
    agg_dict = {}
    for cname in slot_columns:
        df_new = df.copy()
        df_new, paths, agg_points = reassign_cluster_heads(df_new, cname)
        col_paths[cname] = paths
        agg_dict[cname] = agg_points
        # print(df_new[["cluster", "parent", "new_parent", "route", "new_route"]][
        #          (df_new["new_parent"] == 0) | (df_new["parent"] == 0)].sort_values(["cluster"]))
        df_new = update_columns_after_reassignment(df_new)
        df_stats = evaluate(df_new, coords)
        eval_matrix_slots[cname] = df_stats.copy()
    return eval_matrix_slots, col_paths, agg_dict


def update_columns_after_reassignment(df):
    # print("Updating columns 'parent' and 'route'")
    df["parent"] = df["new_parent"]
    df["route"] = df["new_route"]
    df = df.drop(columns=["new_parent", "new_route"])
    return df
