

POST https://{apic}/api/node/mo/uni/fabric/outofsvc.json

{
  "fabricRsDecommissionNode": {
    "attributes": {
      "tDn": "topology/pod-1/node-101",
      "status": "created,modified",
      "removeFromController": "false"
    }
  }
}

POST https://{apic}/api/node/mo/uni/fabric/outofsvc.json

{
  "fabricRsDecommissionNode": {
    "attributes": {
      "tDn": "topology/pod-1/node-101",
      "status": "deleted"
    }
  }
}

POST https://{apic}/api/node/mo/uni/controller/nodeidentpol.json

{
  "fabricNodeIdentP": {
    "attributes": {
      "serial": "FDO12345678",
      "nodeId": "101",
      "name": "leaf-101",
      "podId": "1",
      "status": "created"
    }
  }
}