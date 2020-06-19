import random
# import argparse
import time
import numpy as np
import sys as sys

from yafs.core import Sim
from yafs.application import Application, Message
from yafs.population import *
from yafs.topology import Topology
from yafs.stats import Stats
# from yafs.placement import ClusterPlacement, EdgePlacement, NoPlacementOfModules, Placement
from yafs.distribution import deterministicDistribution, deterministicDistributionStartPoint
from yafs.utils import fractional_selectivity

from UCSelection import MinimunPath, MinPath_RoundRobin
from UCPlacement import UCPlacement

# isso foi inserido para ler a classe que criei em outro arquivo
sys.path.append(".")

RANDOM_SEED = 1


def create_application(name, params=None):
    # APLICATION
    a = Application(name=name, params=params)

    # MODULES (face detection, feature extraction, face recognition)
    a.set_modules([{"Camera": {"Type": Application.TYPE_SOURCE}},
                   {"MLO": {"RAM": 256, "Type": Application.TYPE_MODULE}},
                   {"Broker": {"RAM": 256, "Type": Application.TYPE_MODULE}},
                   {"FLO": {"RAM": 256, "Type": Application.TYPE_MODULE}},
                   {"DLO": {"Type": Application.TYPE_SINK}}
                   ])

    # MESSAGES
    m_cam_mlo = Message("RawVideo", "Camera", "MLO", instructions=100 * 10 ^ 6, bytes=20000)
    m_mlo_brk = Message("ObjectDetected", "MLO", "Broker", instructions=5 * 10 ^ 6, bytes=2000)
    m_brk_flo = Message("IdentifyObject", "Broker", "FLO", instructions=50 * 10 ^ 6, bytes=2000)
    m_flo_brk = Message("ObjectIdentified", "FLO", "Broker", instructions=2 * 10 ^ 6, bytes=100)
    m_brk_dlo = Message("EventIdentified", "Broker", "DLO", instructions=5 * 10 ^ 6, bytes=2000)

    # Add in the application those messages that come from pure sources (sensors).
    # This distinction allows them to be controlled by the (:mod:`Population`) algorithm
    a.add_source_messages(m_cam_mlo)

    # dDistribution = deterministicDistribution(name="Deterministic", time=50)
    # Which module will start processing services and in which distribution and message
    # a.add_service_source("Camera", dDistribution, m_cam_mlo)

    # MODULE SERVICES
    a.add_service_module("MLO", message_in=m_cam_mlo, message_out=m_mlo_brk,
                         distribution=fractional_selectivity, threshold=0.5)  # probability to yield message_out
    a.add_service_module("Broker", message_in=m_mlo_brk, message_out=m_brk_flo)
    a.add_service_module("FLO", message_in=m_brk_flo, message_out=m_flo_brk)
    a.add_service_module("Broker", message_in=m_flo_brk, message_out=m_brk_dlo)
    a.add_service_module("DLO", message_in=m_brk_dlo)

    return a


def create_json_topology(numMLO, numBroker, numFLO, numDLO):
    ## MANDATORY FIELDS

    # TODO: Create dynamically the topology based on the number of nodes

    topology_json = {"entity": [], "link": []}

    id = 0
    S_dev = {"id": id, "type": "Source", "model": "camera", "name": "S",
             "IPT": 10 * 10 ^ 6, "RAM": 100, "COST": 3, "WATT": 30.0}
    topology_json["entity"].append(S_dev)

    id = 1
    for _ in range(numMLO):
        dev = {"id": id, "type": "MLO", "model": "node", "name": "MLO"+str(id),
                   "IPT": 10 * 10 ^ 6, "RAM": 4000, "COST": 3, "WATT": 40.0}
        topology_json["entity"].append(dev)
        id += 1

    idFirstBroker = id
    for _ in range(numBroker):
        dev = {"id": id, "type": "Broker", "model": "node", "name": "Broker"+str(id),
                   "IPT": 10 * 10 ^ 6, "RAM": 4000, "COST": 3, "WATT": 20.0}
        topology_json["entity"].append(dev)
        id += 1

    idFirstFLO = id
    for _ in range(numFLO):
        dev = {"id": id, "type": "FLO", "model": "node", "name": "FLO"+str(id),
                   "IPT": 10 * 10 ^ 6, "RAM": 4000, "COST": 3, "WATT": 20.0}
        topology_json["entity"].append(dev)
        id += 1

    idFirstDLO = id
    for _ in range(numDLO):
        dev = {"id": id, "type": "DLO", "model": "node", "name": "DLO"+str(id),
                   "IPT": 10 * 10 ^ 6, "RAM": 4000, "COST": 3, "WATT": 20.0}
        topology_json["entity"].append(dev)
        id += 1

    # Links among source (node 0) and MLO nodes (first node is 1)
    for i in range(1, 1+numMLO):
        link = {"s": 0, "d": i, "BW": 100, "PR": 2}
        topology_json["link"].append(link)

    # Links among MLO and Broker nodes
    for i in range(1, 1+numMLO):
        for j in range(idFirstBroker, idFirstBroker+numBroker):
            link = {"s": i, "d": j, "BW": 100, "PR": 2}
            topology_json["link"].append(link)

    # Full-duplex links among Broker and FLO nodes
    for i in range(idFirstBroker, idFirstBroker+numBroker):
        for j in range(idFirstFLO, idFirstFLO+numFLO):
            link = {"s": i, "d": j, "BW": 100, "PR": 2}
            topology_json["link"].append(link)
            link = {"s": j, "d": i, "BW": 100, "PR": 2}
            topology_json["link"].append(link)

    # Links among Broker and DLO nodes
    for i in range(idFirstBroker, idFirstBroker+numBroker):
        for j in range(idFirstDLO, idFirstDLO+numDLO):
            link = {"s": i, "d": j, "BW": 100, "PR": 2}
            topology_json["link"].append(link)

    return topology_json


# @profile
def main(simulated_time):
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    """
    TOPOLOGY from a json
    """
    topology = Topology()
    t_json = create_json_topology(numMLO=3, numBroker=1, numFLO=2, numDLO=1)
    topology.load(t_json)
    # t.write("network.gexf")

    """
    APPLICATIONS
    """
    app = create_application(name="Workflow1", params={"max_latency": 100, "FPS_total": 120})
    app2 = create_application(name="Workflow2", params={"max_latency": 100, "FPS_total": 120})

    """
    PLACEMENT algorithm
    """
    # MELINDA's placement algorithm
    # It uses model:node
    placement = UCPlacement(name="UCPlacement")

    # Which services will be necessary
    placement.scaleService({"MLO": 3, "Broker": 1, "FLO": 2, "DLO": 1})

    """
    POPULATION algorithm
    """
    # In ifogsim, during the creation of the application, the Sensors are assigned to the topology,
    # in this case no. As mentioned, YAFS differentiates the adaptive sensors and their topological assignment.
    # In their case, the use a statical assignment.
    pop = Statical("Statical")
    # For each type of sink modules we set a deployment on some type of devices
    # A control sink consists on:
    #  args:
    #     model (str): identifies the device or devices where the sink is linked
    #     number (int): quantity of sinks linked in each device
    #     module (str): identifies the module from the app who receives the messages
    pop.set_sink_control({"model": "sink", "number": 1, "module": app.get_sink_modules()})

    # In addition, a source includes a distribution function:
    dDistribution = deterministicDistribution(name="Deterministic", time=5)
    # delayDistribution = deterministicDistributionStartPoint(400, 100, name="DelayDeterministic")

    # number:quantidade de replicas
    pop.set_src_control({"model": "camera", "number": 1, "message": app.get_message("RawVideo"),
                         "distribution": dDistribution})
    # pop.set_src_control({"type": "mlt", "number":1,"message": app.get_message("M.MLT"),
    #                     "distribution": dDistribution})

    """
    SELECTOR algorithm
    """
    # Their "selector" is actually the shortest way, there is not type of orchestration algorithm.
    # This implementation is already created in selector.class,called: First_ShortestPath
    # selectorPath = MinimunPath()
    selectorPath = MinPath_RoundRobin()

    """
    SIMULATION ENGINE
    """

    stop_time = simulated_time
    s = Sim(topology, default_results_path="Results")
    s.deploy_app(app, placement, pop, selectorPath)
    s.run(stop_time, show_progress_monitor=False)

    s.draw_allocated_topology()  # for debugging


if __name__ == '__main__':
    import logging.config
    # import os

    logging.config.fileConfig('/Users/aluiziorocha/opt/anaconda3/yafs/logging.ini')

    start_time = time.time()
    # duracao a simulacao
    simulated_time = 100
    main(simulated_time=simulated_time)

    print("\n--- %s seconds ---" % (time.time() - start_time))

    ### Finally, you can analyse the results:
    print "-" * 20
    print "Results:"
    print "-" * 20
    m = Stats(defaultPath="Results")  # Same name of the results
    time_loops = [["RawVideo", "ObjectDetected", "IdentifyObject", "ObjectIdentified", "EventIdentified"]]
    # TODO: COLOCAR AS OUTRAS MENSAGENS EM time_loops
    m.showResults2(simulated_time, time_loops=time_loops)
    m.df["date"] = m.df.time_in.astype('datetime64[s]')
    m.df.index = m.df.date

    print "Media da latencia a cada 10 segundos"
    print m.df.resample('10s').agg(dict(time_latency='mean'))
    print "\t- Network saturation -"
    print "\t\tAverage waiting messages : %i" % m.average_messages_not_transmitted()
    print "\t\tPeak of waiting messages : %i" % m.peak_messages_not_transmitted()
    print "\t\tTOTAL messages not transmitted: %i" % m.messages_not_transmitted()

    # print "\n\t- Stats of each service deployed -"
    # print m.get_df_modules()
    # print "-------------------"
    # print m.get_df_service_utilization("ProcessingTask",1000)
    # print m.get_df_service_utilization("Sink",1000)
