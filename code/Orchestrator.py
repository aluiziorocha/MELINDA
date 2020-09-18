# This is the Orchestrator component managing the nodes allocation and deploying workflows

from statistics import stdev

# Weights (as costs) to select the best nodes combination to attend a workflow demand
waste_weight = 4.0
num_nodes_weight = 2.0
std_fd_weight = 1.0

# Idle nodes available for processing a new workflow
nodes = [{'edgeNodeId': '5f2484b37c803900291ea3d0', 'mlo': 5, 'flo': 20, 'dlo': 30},
         {'edgeNodeId': '5f2484b37c803900291ea3d1', 'mlo': 1, 'flo': 10, 'dlo': 50},
         {'edgeNodeId': '5f2484b37c803900291ea3d2', 'mlo': 10, 'flo': 25, 'dlo': 90},
         {'edgeNodeId': '5f2484b37c803900291ea3d3', 'mlo': 40, 'flo': 30, 'dlo': 10},
         {'edgeNodeId': '5f2484b37c803900291ea3d4', 'mlo': 3, 'flo': 20, 'dlo': 10},
         {'edgeNodeId': '5f2484b37c803900291ea3d5', 'mlo': 20, 'flo': 10, 'dlo': 10}]

# Workflow demand in FPS
workflow_fps_demand = 45


def SelectNodes(fps_demand, nodes):
    nodes = sorted(nodes.items(), key=lambda x: x[1], reverse=True)
    solutions = []
    for pivot in range(len(nodes)):
        sum_capacity = nodes[pivot][1]
        partial_solution = [nodes[pivot]]
        if sum_capacity >= fps_demand:
            solutions.append(partial_solution)
        else:
            for i in range(pivot + 1, len(nodes)):
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
                cap = sum_capacity + nodes[i][1]
                if cap >= fps_demand:
                    possible_solution = partial_solution.copy()
                    possible_solution.append(nodes[i])
                    solutions.append(possible_solution)
                else:
                    sum_capacity += nodes[i][1]
                    partial_solution.append(nodes[i])

    if len(solutions) > 0:
        best_solution = best_solution_cost = 99999
        for i, combination in enumerate(solutions):
            total_fps = sum([element[1] for element in combination])
            waste = total_fps - workflow_fps_demand
            num_nodes = len(combination)
            frames_distribution = 0 if len(combination) == 1 else stdev([element[1] for element in combination])
            cost = waste * waste_weight + num_nodes * num_nodes_weight + frames_distribution * std_fd_weight
            if cost < best_solution_cost:
                best_solution_cost = cost
                best_solution = i
        return solutions[best_solution]
    else:
        return solutions


def main():
    mlo_nodes = {}
    for node in nodes:
        mlo_nodes[node['edgeNodeId']] = node['mlo']
    mlo_nodes_selected = SelectNodes(workflow_fps_demand, mlo_nodes)

    flo_nodes = {}
    for node in nodes:
        if node['edgeNodeId'] not in [x[0] for x in mlo_nodes_selected]:
            flo_nodes[node['edgeNodeId']] = node['flo']
    flo_nodes_selected = SelectNodes(workflow_fps_demand, flo_nodes)

    dlo_nodes = {}
    for node in nodes:
        if node['edgeNodeId'] not in [x[0] for x in mlo_nodes_selected] and \
           node['edgeNodeId'] not in [x[0] for x in flo_nodes_selected]:
            dlo_nodes[node['edgeNodeId']] = node['dlo']
    dlo_nodes_selected = SelectNodes(workflow_fps_demand, dlo_nodes)
    print("Nodes selected to attend a workflow demanding {} FPS of processing capacity:".format(workflow_fps_demand))
    print("MLO operators: {}".format(mlo_nodes_selected))
    print("FLO operators: {}".format(flo_nodes_selected))
    print("DLO operators: {}".format(dlo_nodes_selected))


if __name__ == "__main__":
    main()
