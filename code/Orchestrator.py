# This is the Orchestrator component managing the nodes allocation and deploying workflows
import argparse
from math import inf
from os import path


def Solutions(fps_demand, nodes):
    nodes.sort(key=lambda x: x[1], reverse=True)
    solutions = []
    k = len(nodes)
    for pivot in range(k):
        sum_capacity = nodes[pivot][1]
        partial_solution = [nodes[pivot]]
        if sum_capacity >= fps_demand:
            solutions.append(partial_solution)
        else:
            for i in range(pivot + 1, k):
                # --- Searching a node with remaining capacity
                cap = fps_demand - sum_capacity
                try:
                    index = [x[1] for x in nodes[i:]].index(cap) + i
                except ValueError:
                    index = None
                if index is not None:
                    sum_capacity += nodes[index][1]
                    partial_solution.append(nodes[index])
                    solutions.append(partial_solution)
                    break
                # -----------
                cap = sum_capacity + nodes[i][1]
                if cap >= fps_demand:
                    possible_solution = partial_solution.copy()
                    possible_solution.append(nodes[i])
                    solutions.append(possible_solution)
                else:
                    sum_capacity += nodes[i][1]
                    partial_solution.append(nodes[i])
    return solutions


def SelectNodes(fps_demand, nodes, A, B, C, D):
    solutions = Solutions(fps_demand, nodes)
    if len(solutions) > 0:
        best_solution = best_solution_cost = inf
        for i, combination in enumerate(solutions):
            total_fps = sum([element[1] for element in combination])
            wst = total_fps - fps_demand
            lat = sum([element[2] for element in combination])
            ene = sum([element[3] for element in combination])
            ava = sum([1 - element[4] for element in combination])
            cost = wst * A + lat * B + ene * C + ava * D
            if cost < best_solution_cost:
                best_solution_cost = cost
                best_solution = i
        return solutions[best_solution], best_solution_cost
    else:
        return solutions, 0


def main(args):
    if path.isfile(args["datafile"]):
        nodes = []
        with open(args['datafile']) as fp:
            line = fp.readline()
            while line:
                col = line.split()
                if len(col) > 1:
                    nodes.append((col[0], int(col[1]), int(col[2]), int(col[3]), float(col[4])))
                line = fp.readline()

    # Workflow demand in FPS
    workflow_fps_demand = args["fps"]

    # Weights prioritizing QoS requirements
    A = args["A"]
    B = args["B"]
    C = args["C"]
    D = args["D"]

    nodes_selected, cost = SelectNodes(workflow_fps_demand, nodes, A, B, C, D)
    print("To process {} FPS, cost is {} with the following nodes: {}".format(workflow_fps_demand, cost,
                                                                              [x[0] for x in nodes_selected]))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-df", "--datafile", required=True, help="File containing nodes data")
    ap.add_argument("-f", "--fps", type=int, required=True, help="Workflow demand in FPS")
    ap.add_argument("-a", "--A", type=float, default=0.25, help="Coefficient A")
    ap.add_argument("-b", "--B", type=float, default=0.25, help="Coefficient B")
    ap.add_argument("-c", "--C", type=float, default=0.25, help="Coefficient C")
    ap.add_argument("-d", "--D", type=float, default=0.25, help="Coefficient D")
    args = vars(ap.parse_args())

    main(args)
