"""
    This type of algorithm have two obligatory functions:

        *initial_allocation*: invoked at the start of the simulation

        *run* invoked according to the assigned temporal distribution.

"""

from yafs.placement import Placement


class UCPlacement(Placement):
    """
    This implementation locates the services of the application in the cheapest cloud regardless of where the sources or sinks are located.

    It only runs once, in the initialization.

    """

    def initial_allocation(self, sim, app_name):
        # We find the ID-nodo/resource
        # value = {"model": "node"}  # or whatever tag
        # id_nodes = sim.topology.find_IDs(value)

        id_mlo = sim.topology.find_IDs({"type": "MLO"})
        id_brk = sim.topology.find_IDs({"type": "Broker"})
        id_flo = sim.topology.find_IDs({"type": "FLO"})
        id_dlo = sim.topology.find_IDs({"type": "DLO"})

        app = sim.apps[app_name]
        services = app.services
        for module in services:
            # print"-----####------#####------####-----###"
            # print(module)
            if module == "MLO":
                idDES = sim.deploy_module(app_name, module, services[module], id_mlo)
            if module == "Broker":
                idDES = sim.deploy_module(app_name, module, services[module], id_brk)
            if (module == "FLO"):
                idDES = sim.deploy_module(app_name, module, services[module], id_flo)
            if module == "DLO":
                idDES = sim.deploy_module(app_name, module, services[module], id_dlo)
            # if module in self.scaleServices:
            # for rep in range(0, self.scaleServices[module]):
            # idDES = sim.deploy_module(app_name,module,services[module],id_flt)

    # end function
