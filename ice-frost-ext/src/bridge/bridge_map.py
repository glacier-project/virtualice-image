"""Frost data models <-> the real ICE line, for KafkaBridge.lf.

- which variables of each Frost machine the real line publishes on Kafka (kafka/contract.json), and
  the line it publishes for them (utils/kafka_contract.contract_line);
- where a RabbitMQ RPC request of ICE Teach goes: requests use the OPC UA paths of the real servers.

The ice-frost data models follow the real servers, under their own root: OPC UA path = "Objects/" +
Frost path, except the roots in REAL_ROOT.
"""

from machine_data_model.builder.data_model_builder import DataModelBuilder
from machine_data_model.nodes.folder_node import FolderNode
from machine_data_model.nodes.method_node import MethodNode
from machine_data_model.nodes.variable_node import ObjectVariableNode, VariableNode

from kafka_contract import contract_line

REAL_ROOT = {"Objects": "Objects", "Cell1": "Objects", "Cell2": "Objects/OPCUA"}


def real_path(frost_path: str) -> str:
    root, _, rest = frost_path.strip("/").partition("/")
    return f"{REAL_ROOT[root]}/{rest}" if root in REAL_ROOT else f"Objects/{root}/{rest}"


def nodes(data_model_path: str):
    """(variable paths, method paths) of a Frost data model, root included ("Warehouse/Status/trayInBay").
    Properties of object variables count as variables (".../Pallet7/destination")."""
    dm = DataModelBuilder().get_data_model(data_model_path)
    variables, methods = [], []

    def walk(node, path):
        for child in node:
            p = f"{path}/{child.name}"
            if isinstance(child, (FolderNode, ObjectVariableNode)):
                walk(child, p)
            elif isinstance(child, VariableNode):
                variables.append(p)
            elif isinstance(child, MethodNode):
                methods.append(p)
    walk(dm.root, dm.root.name)
    return variables, methods


class LineMap:
    """machines: {frost machine name: (data model path, contract topic prefix, e.g. "ice_data_cell6")}."""

    def __init__(self, machines: dict, contract: dict):
        self.contract = contract
        self.prefix = {m: prefix for m, (_, prefix) in machines.items()}
        self.published = {}  # machine -> Frost paths of the variables the real line publishes
        self.by_real = {}    # OPC UA path -> [(machine, Frost path)]  (variables and methods)
        for machine, (dm_path, prefix) in machines.items():
            variables, methods = nodes(dm_path)
            self.published[machine] = [v for v in variables if self.line(machine, v, 0, 0)]
            for p in variables + methods:
                self.by_real.setdefault(real_path(p), []).append((machine, p))

    def line(self, machine: str, frost_path: str, value, ts: int):
        """(topic, line) of the real line for this variable, None if the line never publishes it."""
        if value is None:
            return None
        return contract_line(self.contract, self.prefix[machine], real_path(frost_path), value, ts)

    def route(self, path: str, queue: str):
        """(machine, Frost path) for an RPC request on `queue` about OPC UA `path`, None if unknown.
        Same path on several machines (the two Kairos): the one whose topic prefix ends with a word
        of the queue name ("rbkairos_a_rpc_queue" -> ice_data_rbkairos_a)."""
        hits = self.by_real.get(path.strip("/")) or self.by_real.get("Objects/" + path.strip("/"))
        if not hits:
            return None
        if len(hits) > 1:
            hits = [h for h in hits if self.prefix[h[0]].removeprefix("ice_data_") in queue] or hits
        return hits[0]


def rpc_reply(correlation_id: str, value, ok: bool = True) -> dict:
    """Body of the RPC answer ICE Teach reads (RabbitClient.RpcRead returns the whole object and
    CheckVariable reads value: a scalar, see ice_teach/patch_ice_teach.py D1)."""
    return {"key": correlation_id, "body": {"result": ok, "value": value, "returns": {}}, "value": value}
