

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


APIC="https://apic1"

# ログイン
curl -sk -X POST "$APIC/api/aaaLogin.json" \
  -d '{"aaaUser":{"attributes":{"name":"admin","pwd":"****"}}}' -c cookie.txt

# 新規登録
curl -sk -X POST "$APIC/api/node/mo/uni/controller/nodeidentpol.json" -b cookie.txt \
  -d '{"fabricNodeIdentP":{"attributes":{"serial":"FDO12345678","nodeId":"101","name":"leaf-101","podId":"1","role":"leaf","status":"created"}}}'

# Decommission(一時)
curl -sk -X POST "$APIC/api/node/mo/uni/fabric/outofsvc.json" -b cookie.txt \
  -d '{"fabricRsDecommissionNode":{"attributes":{"tDn":"topology/pod-1/node-101","status":"created,modified","removeFromController":"false"}}}'

# Recommission
curl -sk -X POST "$APIC/api/node/mo/uni/fabric/outofsvc.json" -b cookie.txt \
  -d '{"fabricRsDecommissionNode":{"attributes":{"tDn":"topology/pod-1/node-101","status":"deleted"}}}'

# 状態確認
curl -sk "$APIC/api/class/fabricNode.json" -b cookie.txt | python3 -m json.tool
