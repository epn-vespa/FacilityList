from typing import List, Dict, Tuple


def merge_into(newer_entity_dict: Dict,
               prior_entity_dict: Dict):
    """
    Merge data from the prior dict into the newer dict.
    Only keep the most precise latitude & longitude.
    The prior entity dict will not be modified, only the
    newer entity dict will be augmented with the prior entity
    dict's keys and values.

    Args:
        newer_entity_dict: the entity dict to save data in
        prior_entity_dict: the prior entity dict to merge into the newer
    """
    for key, values in prior_entity_dict.copy().items():
        if key == "prior_id":
            continue
        if key == "label":
            if not isinstance(values, set):
                values = {values}
            if "alt_label" in newer_entity_dict:
                 # Keep the old label as an alternate label of the new entity
                if type(newer_entity_dict["alt_label"]) == list:
                    newer_entity_dict["alt_label"] = set(newer_entity_dict["alt_label"])
                # set
                newer_entity_dict["alt_label"].update(values)
            else:
                newer_entity_dict["alt_label"] = values
        elif key in newer_entity_dict:
            merge_into = newer_entity_dict[key]
            if not isinstance(values, list) and not isinstance(values, set):
                values = [values]
            for value in values:
                if isinstance(merge_into, set):
                    merge_into.add(value)
                    continue
                elif not isinstance(merge_into, list):
                    merge_into = [merge_into]
                if value not in merge_into:
                    if key in ["latitude", "longitude"]:
                        # Keep the most precise value
                        old_value = newer_entity_dict[key]
                        if isinstance(old_value, list):
                            old_value = old_value[0]
                        if (len(str(value)) > len(str(old_value)) and
                            str(value).startswith(str(old_value))):
                            merge_into = [value]
                        elif (len(str(old_value)) > len(str(value)) and
                              str(old_value).startswith(str(value))):
                            merge_into = [old_value]
                        elif len(str(value)) == len(str(old_value)):
                            if value != old_value:
                                merge_into = [value, old_value] # Keep both
                            else:
                                merge_into = [old_value]
                        elif value != old_value:
                            # Keep both
                            merge_into = [value, old_value]
                    else:
                        merge_into.append(value)
            newer_entity_dict[key] = merge_into
        else:
            newer_entity_dict[key] = values
        if "alt_label" in newer_entity_dict and "label" in newer_entity_dict:
            # Prevent label to be in alt_label.
            if type(newer_entity_dict["alt_label"]) == str:
                newer_entity_dict["alt_label"] = {newer_entity_dict["alt_label"]}
            newer_entity_dict["alt_label"] -= {newer_entity_dict["label"]}


def extract_items(d: Dict,
                  parent: str = "") -> List[Tuple]:
    """
    Flatten a recursive dictionary to a list of (key, value).
    This is necessary to create triplets from for json format.
    /!\ Do not extract properties that belong to the broader entity. (FIXME)

    Args:
        d: a recursive dictionary.
        parent: the parent XML div type.
    """
    result = []
    for key, value in d.items():
        if isinstance(value, dict):
            result.extend(extract_items(value, parent = key))
        else:
            if parent == "InformationURL" and key != "URL":
                # SPASE: ignore every side information about the url.
                continue
            result.append((key, value))
    return result


class UnionFind:
    """
    Disjoint Set Union (Union-Find) data structure.

    Maintains a collection of disjoint sets over arbitrary hashable elements.
    Supports efficient union and find operations with:
      - path compression (during find)
      - union by rank (tree height heuristic)

    Amortized time complexity per operation is effectively O(1).
    """

    def __init__(self):
        # parent[x] = parent of x in the union-find tree.
        # If parent[x] == x, then x is the representative (root) of its set.
        self.parent = {}

        # rank[x] = an upper bound on the height of the tree rooted at x.
        # Used only to decide which root becomes the parent during union.
        self.rank = {}

    def find(self, x):
        """
        Find the representative (root) of the set containing x.

        This method applies path compression:
        after the call, x (and all nodes visited on the path)
        will point directly to the root, flattening the tree
        and speeding up future operations.
        """
        # Lazy initialization: if x has never been seen before,
        # create a new singleton set {x}.
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            return x

        # If x is not its own parent, recursively find the root
        # and compress the path by updating parent[x].
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])

        return self.parent[x]

    def union(self, x, y):
        """
        Merge the two sets containing x and y.

        Uses union by rank:
        - the root with smaller rank is attached under the root
          with larger rank
        - if ranks are equal, one root is chosen arbitrarily and
          its rank is incremented

        If x and y are already in the same set, this is a no-op.
        """
        # Find the representatives of both elements
        rx = self.find(x)
        ry = self.find(y)

        # Already in the same set → nothing to do
        if rx == ry:
            return

        # Attach the smaller-rank tree under the larger-rank tree
        if self.rank[rx] < self.rank[ry]:
            self.parent[rx] = ry
        elif self.rank[rx] > self.rank[ry]:
            self.parent[ry] = rx
        else:
            # Same rank: choose one root and increase its rank
            self.parent[ry] = rx
            self.rank[rx] += 1
