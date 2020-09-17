# This is the Orchestrator component managing the nodes allocation and deploying workflows

# Idle nodes
from statistics import stdev

# Weights to select the best nodes combination to attend a workflow demand
waste_weight = 4.0
num_nodes_weight = 2.0
std_fd_weight = 1.0

# Idle nodes available for processing a new workflow
nodes = [{'edgeNodeId': '5f2484b37c803900291ea3d0', 'mlo': 40, 'flo': 20, 'dlo': 30},
         {'edgeNodeId': '5f2484b37c803900291ea3d1', 'mlo': 20, 'flo': 10, 'dlo': 50},
         {'edgeNodeId': '5f2484b37c803900291ea3d2', 'mlo': 10, 'flo': 25, 'dlo': 90},
         {'edgeNodeId': '5f2484b37c803900291ea3d3', 'mlo': 5, 'flo': 30, 'dlo': 10},
         {'edgeNodeId': '5f2484b37c803900291ea3d4', 'mlo': 3, 'flo': 20, 'dlo': 10},
         {'edgeNodeId': '5f2484b37c803900291ea3d5', 'mlo': 1, 'flo': 10, 'dlo': 10}]

# Workflow demand in FPS
workflow_fps_demand = 46


def SelectNodes(fps_demand, nodes):
    nodes = sorted(nodes.items(), key=lambda x: x[1], reverse=True)
    solutions = []
    for pivot in range(len(nodes)):
        sum_capacity = nodes[pivot][1]
        possible_solution = [nodes[pivot]]
        if sum_capacity >= fps_demand:
            solutions.append(possible_solution)
        else:
            i = pivot + 1
            while i < len(nodes) or sum_capacity >= fps_demand:
                if sum_capacity < fps_demand:
                    cap = fps_demand - sum_capacity
                    try:
                        index = [x[1] for x in nodes[i:]].index(cap) + i
                    except ValueError:
                        index = None
                    if index is not None:
                        sum_capacity += nodes[index][1]
                        possible_solution.append(nodes[index])
                    else:
                        sum_capacity += nodes[i][1]
                        possible_solution.append(nodes[i])
                    i += 1
                else:
                    solutions.append(possible_solution)
                    sum_capacity = nodes[pivot][1]
                    possible_solution = [nodes[pivot]]

    return solutions


def main():
    u_nodes = {}
    for node in nodes:
        u_nodes[node['edgeNodeId']] = node['mlo']

    combinations = SelectNodes(workflow_fps_demand, u_nodes)
    best_solution = best_solution_cost = 99999
    for i, combination in enumerate(combinations):
        total_fps = sum([element[1] for element in combination])
        waste = total_fps - workflow_fps_demand
        num_nodes = len(combination)
        frames_distribution = 0 if len(combination) == 1 else stdev([element[1] for element in combination])
        cost = waste * waste_weight + num_nodes * num_nodes_weight + frames_distribution * std_fd_weight
        if cost < best_solution_cost:
            best_solution_cost = cost
            best_solution = i
        print("Combination {}: {}".format(i, combination))
        print("Total capacity: {} FPS".format(total_fps))
        print("Capacity waste: {} FPS".format(waste))
        print("Number of nodes: {}".format(num_nodes))
        print("Std deviation of frames distribution: {:.3f}".format(frames_distribution))
        print("----------------------------------------------")
        print("Total cost for this combination: {:.3f}".format(cost))
        print("==============================================")
    print("Best combination of nodes to attend the workflow demand ({} FPS): {}".format(workflow_fps_demand,
                                                                                        best_solution))


if __name__ == "__main__":
    main()
