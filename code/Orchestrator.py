# This is the Orchestrator component managing the nodes allocation and deploying workflows
import argparse
from math import inf
from os import path
from numpy import product
import datetime


def WorstFit(fps_demand, nodes):
    nodes.sort(key=lambda x: x[1], reverse=False)
    solutions = []
    sum_capacity = 0
    partial_solution = []
    for node in nodes:
        sum_capacity += node[1]
        partial_solution.append(node)
        if sum_capacity >= fps_demand:
            solutions.append(partial_solution)
            break
    return solutions


def BestFit(fps_demand, nodes):
    nodes.sort(key=lambda x: x[1], reverse=True)
    solutions = []
    sum_capacity = 0
    partial_solution = []
    for node in nodes:
        sum_capacity += node[1]
        partial_solution.append(node)
        if sum_capacity >= fps_demand:
            solutions.append(partial_solution)
            break
    return solutions


def FirstFit(fps_demand, nodes):
    solutions = []
    sum_capacity = 0
    partial_solution = []
    for node in nodes:
        sum_capacity += node[1]
        partial_solution.append(node)
        if sum_capacity >= fps_demand:
            solutions.append(partial_solution)
            break
    return solutions


def MELINDA(fps_demand, nodes):
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


def SelectNodes(method, write, fps_demand, nodes, A, B, C, D):
    if method == 0:
        solutions = MELINDA(fps_demand, nodes)
    elif method == 1:
        solutions = FirstFit(fps_demand, nodes)
    elif method == 2:
        solutions = BestFit(fps_demand, nodes)
    elif method == 3:
        solutions = WorstFit(fps_demand, nodes)
    else:
        print("[ERROR] Invalid method!")
        return []
    # print("We found ", len(solutions), " solution(s)")
    if len(solutions) > 0:
        if write == 1:
            timestamp = datetime.datetime.now()
            filename = "allSolutions_" + str(method) + "_" + timestamp.strftime('%Y-%m-%d_%H-%M-%S-%f') + ".csv"
            f = open(filename, "w")
            f.write("Total Cost".rjust(20) + "\t Capacity".rjust(10) + "\t Latency".rjust(10) + "\t Energy".rjust(
                10) + "\t Combination\n")
        best_solution = best_solution_cost = inf
        for i, combination in enumerate(solutions):
            total_fps = sum([element[1] for element in combination])
            # Capacity waste
            wst = total_fps - fps_demand
            # Max latency
            lat = max([element[2] for element in combination])
            # Energy consumption
            ene = sum([element[3] for element in combination])
            # Unavailability rate
            unava = product([1 - element[4] for element in combination])
            # Total cost
            cost = wst * A + lat * B + ene * C + unava * D
            if write == 1:
                f.write(repr(cost).rjust(20) + "\t" + str(total_fps).rjust(10) + "\t" + str(lat).rjust(10) + "\t" + str(
                    ene).rjust(10) + "\t" + str([element[1] for element in combination]) + "\n")
            if cost < best_solution_cost:
                best_solution_cost = cost
                best_solution = i
        if write == 1:
            f.close()
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
                    nodes.append((col[0], int(col[1]), int(col[2]), float(col[3]), float(col[4])))
                line = fp.readline()
    else:
        print("Datafile not found!")
        exit(1)

    # Workflow demand in FPS
    workflow_fps_demand = args["fps"]

    # Weights prioritizing QoS requirements
    A = args["A"]
    B = args["B"]
    C = args["C"]
    D = args["D"]

    workflow = 1
    while True:
        nodes_selected, cost = SelectNodes(args["method"], args["write"], workflow_fps_demand,
                                           nodes, A, B, C, D)
        if cost != 0:
            print("Workflow: {}, cost: {}, nodes: {}".format(workflow, cost,
                                                             [x[0] for x in nodes_selected]))
            # print("{}\t{}".format(workflow, cost))
            workflow += 1
            for n in nodes_selected:
                nodes.remove(n)
        else:
            break
    if len(nodes) > 0:
        print("Nodes remaining: {}".format([x for x in nodes]))
    else:
        print("No nodes remaining")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-m", "--method", type=int, default=0,
                    help="Method to use: 0 - MELINDA (default), 1 - First-fit, 2 - Best-fit, 3 - Worst-fit")
    ap.add_argument("-df", "--datafile", required=True, help="File containing nodes data")
    ap.add_argument("-f", "--fps", type=int, required=True, help="Workflow demand in FPS")
    ap.add_argument("-a", "--A", type=float, default=0.25, help="Coefficient A")
    ap.add_argument("-b", "--B", type=float, default=0.25, help="Coefficient B")
    ap.add_argument("-c", "--C", type=float, default=0.25, help="Coefficient C")
    ap.add_argument("-d", "--D", type=float, default=0.25, help="Coefficient D")
    ap.add_argument("-w", "--write", type=int, default=0, help="Write (1) or not (0-default) solutions to csv file")
    args = vars(ap.parse_args())

    main(args)
