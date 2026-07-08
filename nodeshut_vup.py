import argparse
import os
import uuid
import time
from datetime import datetime
import re
import credentials
import blsw_traffic_check
import random
import threading
import json
import shutil
import result_code
from http import client
import sys
import requests
import subprocess
import psycopg2
import paramiko
import apic_leafs
from typing import Optional, Dict, Any, List
import traceback
import difflib
import urllib3
from typing import Tuple, List

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# このスクリプトは、APIC に対する leaf/spine/apic の shutdown・noshut 投入を
# 自動化し、前後の状態収集や差分判定、ログ・ステータスファイルの生成までを
# 一括で実施するユーティリティ。main() で引数を受け取り、対象ノードごとに
# shutdown/enable のシナリオを実行する。

psql_host = credentials.PSQL_HOST
psql_db = credentials.PSQL_DB
psql_user = credentials.PSQL_USER
psql_password = credentials.PSQL_PASSWORD

apic_username = credentials.USERNAME
apic_password = credentials.PASSWORD

protocol = credentials.PROTOCOL

status_json_lock = threading.Lock()

script_directory = os.path.dirname(os.path.abspath(__file__))


def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_timestamp():
    return datetime.now().strftime("%Y%m%d%H%M%S")


def log_processing(log_directory, pid, message):
    """処理フローの進捗ログ（人間が追う用）"""
    log_path = f"{log_directory}/{pid}_processing.log"
    print(f"{timestamp()} [PROCESSING] {message}")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{timestamp()} [PROCESSING] {message}\n")


def log_detail(log_directory, pid, message):
    """デバッグ用の詳細ログ（調査用の深い情報）"""
    log_path = f"{log_directory}/{pid}_detail.log"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{timestamp()} [DETAIL] {message}\n")


def fail_all_and_exit(
    log_directory,
    uid,
    hostnames,
    message,
    code=result_code.EACH_STATUS_CODE_SERVER_ERROR,
):
    for h in hostnames:
        update_node_status(log_directory, uid, h, code, f"{h}: {message}")
    finalize_status(log_directory, uid)
    sys.exit(1)


def get_token(apic_ip, username, password):
    auth_endpoint = f"{protocol}://{apic_ip}/api/aaaLogin.json"
    session = requests.Session()
    session.verify = False
    auth_info = {"aaaUser": {"attributes": {"name": username, "pwd": password}}}

    try:
        auth_response = session.post(
            auth_endpoint, json=auth_info, proxies={"http": None, "https": None}
        )
        auth_response.raise_for_status()
        token = auth_response.json()["imdata"][0]["aaaLogin"]["attributes"]["token"]
        return token
    except Exception as e:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raise RuntimeError(f"APICログイン失敗 ({apic_ip}): {e}")


def fetch_from_psql(psql_host, psql_db, psql_user, psql_password, query, params=None):
    db_config = {
        "host": f"{psql_host}",
        "database": f"{psql_db}",
        "user": f"{psql_user}",
        "password": f"{psql_password}",
    }

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                if params is not None:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                rows = cur.fetchall()
                return ["" if row[0] is None else str(row[0]) for row in rows]
    except Exception as e:
        print(f"Database error: {e}")
        return []


def check_connection(apic_ips):
    successful_apic_ips = []
    for ip in apic_ips:
        try:
            url = f"{protocol}://{ip}/api/class/topSystem.json"
            http_response = requests.get(url, proxies={"http": None, "https": None}, verify=False, timeout=10)
            if http_response.status_code == 403:
                successful_apic_ips.append(ip)
            else:
                print(f"{ip} responded with status code {http_response.status_code}")
        except requests.exceptions.ConnectionError as e:
            print(f"{ip} connection error: {e}")
        except requests.exceptions.Timeout:
            print(f"{ip} timed out.")
        except Exception as e:
            print(f"Unable to connect to {ip}. Exception: {e}")

    if not successful_apic_ips:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raise RuntimeError(f"{now} No reachable APIC IPs found.")

    apic_ip = random.choice(successful_apic_ips)
    return apic_ip


def analyze_show_module_log(log_path: str) -> Tuple[bool, str]:
    """
    Read a .log file containing 'show module' output and analyze it.

    Returns:
        (True, "")                  → if all modules are OK
        (False, "error message")    → if any module has problems
    """
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            output = f.read()
    except Exception as e:
        return False, f"Failed to read log file '{log_path}': {e}"

    lines = output.splitlines()
    errors = []

    ok_status = {"ok", "active", "standby"}

    # --- Parse main module Status ---
    in_main = False
    for line in lines:
        if line.strip().startswith("Mod") and "Module-Type" in line:
            in_main = True
            continue
        if in_main:
            if not line.strip():
                in_main = False
                continue
            parts = line.split()
            if parts and parts[0].isdigit():
                mod = parts[0]
                status = parts[-1].lower()
                if status not in ok_status:
                    errors.append(f"Module {mod} bad status: {status}")

    # --- Parse Online Diag Status ---
    in_diag = False
    for line in lines:
        if line.strip().startswith("Mod") and "Online Diag Status" in line:
            in_diag = True
            continue
        if in_diag:
            if not line.strip():
                in_diag = False
                continue
            parts = line.split()
            if parts and parts[0].isdigit():
                mod = parts[0]
                diag = parts[-1].lower()
                if diag != "pass":
                    errors.append(f"Module {mod} diag failed: {diag}")

    if errors:
        return False, "; ".join(errors)

    return True, ""


def analyze_diag_result_output(text: str) -> Tuple[bool, str]:
    """
    'show diagnostic result module all detail | egrep "[0-9]\\) |Error code"' の
    出力文字列を解析し、全テストが DIAG TEST SUCCESS か確認する。

    * 先頭に "=====" で始まるタイトル行があればスキップする *
    """
    lines = text.splitlines()

    # 1行目がタイトルならスキップ
    if lines and "=====" in lines[0]:
        lines = lines[1:]

    current_test = None
    errors: List[str] = []
    found_any_error_code = False

    for raw in lines:
        line = raw.rstrip("\n")

        # テスト名行（例: "  1) bios-mem ."）
        if ")" in line and "Error code" not in line:
            current_test = line.strip()
            continue

        # Error code 行
        if "Error code" in line:
            found_any_error_code = True
            if "DIAG TEST SUCCESS" not in line:
                if current_test:
                    errors.append(f"{current_test} -> {line.strip()}")
                else:
                    errors.append(f"(no test name) -> {line.strip()}")
            current_test = None

    if not found_any_error_code:
        return False, "Error code 行が見つかりませんでした（ログフォーマット要確認）"

    if errors:
        return False, "; ".join(errors)

    return True, ""


def analyze_diag_result_log(log_path: str) -> Tuple[bool, str]:
    """
    egrep 済みログファイルを読み、全テストが DIAG TEST SUCCESS か確認する。

    Returns:
        (True, "")
        (False, "error message...")
    """
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
    except Exception as e:
        return False, f"Failed to read diag log '{log_path}': {e}"

    return analyze_diag_result_output(text)


def update_node_status(log_directory, uid, target_node, status_code, message):
    path = f"{log_directory}/{uid}_status.json"
    with status_json_lock:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"{timestamp()} Failed to load status.json: {e}")
            return

        found = False
        for node in data.get("results", []):
            if node["target_node"] == target_node:
                node["each_status_code"] = status_code
                node["message"] = message
                found = True
                break

        if not found:
            print(
                f"{timestamp()} WARNING: Node '{target_node}' not found in status.json"
            )

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


"""def finalize_status(log_directory, uid):
    path = f"{log_directory}/{uid}_status.json"
    with status_json_lock:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"{timestamp()} Failed to finalize status.json: {e}")
            return

        all_error = all(
            n["each_status_code"].startswith("E") for n in data.get("results", [])
        )
        all_success = all(
            n["each_status_code"].startswith("N") for n in data.get("results", [])
        )
        not_all_success = any(
            n["each_status_code"].startswith("N") for n in data.get("results", [])
        )
        if all_error:
            data["status_code"] = result_code.STATUS_CODE_SERVER_ERROR
            data["message"] = "異常終了"
        elif all_success:
            data["status_code"] = result_code.STATUS_CODE_SUCCESS
            data["message"] = "完了"
        elif not_all_success:
            data["status_code"] = result_code.STATUS_CODE_SUCCESS
            data["message"] = "異常終了を含む"
        else:
            data["status_code"] = result_code.STATUS_CODE_SERVER_ERROR
            data["message"] = "不明"

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)"""


def finalize_status(log_directory, uid):
    path = f"{log_directory}/{uid}_status.json"
    with status_json_lock:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"{timestamp()} Failed to finalize status.json: {e}")
            return

        all_success = all(
            n["each_status_code"].startswith("N") for n in data.get("results", [])
        )
        any_error = any(
            n["each_status_code"].startswith("E") for n in data.get("results", [])
        )
        if all_success:
            data["status_code"] = result_code.STATUS_CODE_SUCCESS
            data["message"] = "完了"
        elif any_error:
            data["status_code"] = result_code.STATUS_CODE_SUCCESS
            data["message"] = "異常終了を含む"
        else:
            data["status_code"] = result_code.STATUS_CODE_SERVER_ERROR
            data["message"] = "不明"

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


def set_client_error_status(
    log_directory, uid, hostnames, message, code=result_code.STATUS_CODE_CLIENT_ERROR
):
    os.makedirs(log_directory, exist_ok=True)
    # path = f"{log_directory}/{uid}_status.json"

    json_nodes = [
        {
            "target_node": h,
            "each_status_code": code,
            "message": f"{h}: {message}",
        }
        for h in (hostnames or [])
    ]

    json_data_structure = {
        "status_code": code,
        "message": message,
        "results": json_nodes,
    }

    with open(f"{log_directory}/{uid}_status.json", "w", encoding="utf-8") as f:
        json.dump(json_data_structure, f, ensure_ascii=False, indent=4)


def get_hostname_info(hostname, apic_ip, apic, token):
    topSystem_url = f'{protocol}://{apic_ip}/api/node/class/topSystem.json?query-target-filter=eq(topSystem.name,"{hostname}")'
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(topSystem_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        node_id = response.json()["imdata"][0]["topSystem"]["attributes"]["id"]
        pod_id = response.json()["imdata"][0]["topSystem"]["attributes"]["podId"]
        return node_id, pod_id
    except Exception as e:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{now} Failed to get host info. {e}")
        return None, None


def get_leaf_ports(token, apic_ip, node_id, pod_id):

    # Fetch list of ethpmPhysIf DNs that are operationally up and connected to an EPG.

    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    ethpm_url = (
        f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{node_id}/ethpmPhysIf.json?"
        'query-target-filter=and(and(wcard(ethpmPhysIf.usage,"epg"),eq(ethpmPhysIf.operSt,"up")),'
        'not(wcard(ethpmPhysIf.usage,"sfp-missing"),wcard(ethpmPhysIf.usage,"controller")))'
    )

    ethpm_url2 = (
        f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{node_id}/ethpmPhysIf.json?"
        'query-target-filter=and(and(wcard(ethpmPhysIf.usage,"fabric"),eq(ethpmPhysIf.operSt,"up")),'
        'not(wcard(ethpmPhysIf.usage,"sfp-missing")))'
    )

    try:
        response = session.get(ethpm_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        dn_list = [item["ethpmPhysIf"]["attributes"]["dn"] for item in imdata]

        response = session.get(ethpm_url2, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        spine_dn_list = [item["ethpmPhysIf"]["attributes"]["dn"] for item in imdata]

        return dn_list, spine_dn_list

    except Exception as e:
        print(f"{datetime.now()} Failed to get ethpmPhysIf data: {e}")
        return [], []


def get_apic_ports(token, apic_ip, node_id, pod_id, up_only=False):

    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    if up_only:
        ethpm_url = f'{protocol}://{apic_ip}/api/node/class/ethpmPhysIf.json?query-target-filter=and(eq(ethpmPhysIf.usage,"epg,controller,infra"),and(eq(ethpmPhysIf.operSt,"up")))'
    else:
        ethpm_url = f'{protocol}://{apic_ip}/api/node/class/ethpmPhysIf.json?query-target-filter=eq(ethpmPhysIf.usage,"epg,controller,infra")'

    try:
        response = session.get(ethpm_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        dn_list = [item["ethpmPhysIf"]["attributes"]["dn"] for item in imdata]

        return dn_list

    except Exception as e:
        print(f"{datetime.now()} Failed to get ethpmPhysIf data: {e}")
        return []


def get_spine_admin_statuses(
    token, apic_ip, node_id, pod_id, hostname, dn_list, out_path=None, check_target="up"
):

    check_key = "adminSt"
    api_class = "l1PhysIf"

    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    node_ids = []
    for line in dn_list:
        try:
            nid = line.split("/")[2].split("-")[1]
            if nid not in node_ids:
                node_ids.append(nid)
        except IndexError:
            continue

    results = []
    overall_all_up = True

    for nid in node_ids:
        node_dn_list = [
            line for line in dn_list if line.split("/")[2].split("-")[1] == nid
        ]

        url = f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{nid}/{api_class}.json"

        try:
            response = session.get(
                url, proxies={"http": None, "https": None}, timeout=10
            )
            response.raise_for_status()
            imdata = response.json().get("imdata", [])

            port_map = {
                item[api_class]["attributes"]["id"]: item[api_class]["attributes"].get(
                    check_key, "unknown"
                )
                for item in imdata
                if api_class in item and "attributes" in item[api_class]
            }

            node_statuses = {}
            all_up = True
            for dn in node_dn_list:
                if "[" in dn and "]" in dn:
                    port = dn.split("[", 1)[1].split("]", 1)[0]
                    st = port_map.get(port, "unknown")
                    node_statuses[port] = st
                    if st != check_target:
                        all_up = False

            if not all_up:
                overall_all_up = False

            results.append(
                {
                    "node_id": nid,
                    "pod_id": str(pod_id),
                    "checked_ports": len(node_statuses),
                    f"{check_key}_statuses": node_statuses,
                    "all_ports_up": all_up,
                }
            )

        except Exception as e:
            results.append(
                {
                    "node_id": nid,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "checked_ports": 0,
                    f"{check_key}_statuses": {},
                    "all_ports_up": False,
                    "error": f"{type(e).__name__}: {e}",
                }
            )
            overall_all_up = False

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if out_path is None:
        fname = f"spine_admin_statuses_{hostname}_pod{pod_id}_{ts}.json"
        out_path = os.path.join(os.getcwd(), fname)

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "apic": apic_ip,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "overall_all_ports_up": overall_all_up,
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception as e:
        print(f"{datetime.now()} Failed to write results file '{out_path}': {e}")

    return overall_all_up


def get_spine_oper_statuses(
    token, apic_ip, node_id, pod_id, hostname, dn_list, out_path=None, check_target="up"
):

    check_key = "operSt"
    api_class = "ethpmPhysIf"

    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    node_ids = []
    for line in dn_list:
        try:
            nid = line.split("/")[2].split("-")[1]
            if nid not in node_ids:
                node_ids.append(nid)
        except IndexError:
            continue

    results = []
    overall_all_up = True

    for nid in node_ids:
        node_dn_list = [
            line for line in dn_list if line.split("/")[2].split("-")[1] == nid
        ]

        url = f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{nid}/{api_class}.json"

        try:
            response = session.get(
                url, proxies={"http": None, "https": None}, timeout=10
            )
            response.raise_for_status()
            imdata = response.json().get("imdata", [])

            # Build port → operSt map (parse from dn)
            port_map = {}
            for item in imdata:
                if api_class in item and "attributes" in item[api_class]:
                    attrs = item[api_class]["attributes"]
                    dn_val = attrs.get("dn", "")
                    if "[" in dn_val and "]" in dn_val:
                        port = dn_val.split("[", 1)[1].split("]", 1)[0]
                        port_map[port] = attrs.get(check_key, "unknown")

            node_statuses = {}
            all_up = True
            for dn in node_dn_list:
                if "[" in dn and "]" in dn:
                    port = dn.split("[", 1)[1].split("]", 1)[0]
                    st = port_map.get(port, "unknown")
                    node_statuses[port] = st
                    if st != check_target:
                        all_up = False

            if not all_up:
                overall_all_up = False

            results.append(
                {
                    "node_id": nid,
                    "pod_id": str(pod_id),
                    "checked_ports": len(node_statuses),
                    f"{check_key}_statuses": node_statuses,
                    "all_ports_up": all_up,
                }
            )

        except Exception as e:
            results.append(
                {
                    "node_id": nid,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "checked_ports": 0,
                    f"{check_key}_statuses": {},
                    "all_ports_up": False,
                    "error": f"{type(e).__name__}: {e}",
                }
            )
            overall_all_up = False

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if out_path is None:
        fname = f"spine_oper_statuses_{hostname}_pod{pod_id}_{ts}.json"
        out_path = os.path.join(os.getcwd(), fname)

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "apic": apic_ip,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "overall_all_ports_up": overall_all_up,
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception as e:
        print(f"{datetime.now()} Failed to write results file '{out_path}': {e}")

    return overall_all_up


def get_leaf_admin_statuses(
    token, apic_ip, node_id, pod_id, hostname, dn_list, out_path=None, check_target="up"
):

    check_key = "adminSt"
    api_class = "l1PhysIf"

    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    results = []
    overall_all_up = True

    url = f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{node_id}/{api_class}.json"

    try:
        response = session.get(url, proxies={"http": None, "https": None}, timeout=10)
        response.raise_for_status()
        imdata = response.json().get("imdata", [])

        port_map = {
            item[api_class]["attributes"]["id"]: item[api_class]["attributes"].get(
                check_key, "unknown"
            )
            for item in imdata
            if api_class in item and "attributes" in item[api_class]
        }

        node_statuses = {}
        all_up = True
        for dn in dn_list:
            if "[" in dn and "]" in dn:
                port = dn.split("[", 1)[1].split("]", 1)[0]
                st = port_map.get(port, "unknown")
                node_statuses[port] = st
                if st != check_target:
                    all_up = False

        if not all_up:
            overall_all_up = False

        results.append(
            {
                "node_id": str(node_id),
                "hostname": hostname,
                "pod_id": str(pod_id),
                "checked_ports": len(node_statuses),
                f"{check_key}_statuses": node_statuses,
                "all_ports_up": all_up,
            }
        )

    except Exception as e:
        results.append(
            {
                "node_id": str(node_id),
                "hostname": hostname,
                "pod_id": str(pod_id),
                "checked_ports": 0,
                f"{check_key}_statuses": {},
                "all_ports_up": False,
                "error": f"{type(e).__name__}: {e}",
            }
        )
        overall_all_up = False

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if out_path is None:
        fname = f"leaf_admin_statuses_{hostname}_node{node_id}_pod{pod_id}_{ts}.json"
        out_path = os.path.join(os.getcwd(), fname)

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "apic": apic_ip,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "overall_all_ports_up": overall_all_up,
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception as e:
        print(f"{datetime.now()} Failed to write results file '{out_path}': {e}")

    return overall_all_up


def get_leaf_oper_statuses(
    token, apic_ip, node_id, pod_id, hostname, dn_list, out_path=None, check_target="up"
):

    check_key = "operSt"
    api_class = "ethpmPhysIf"

    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    results = []
    overall_all_up = True

    url = f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{node_id}/{api_class}.json"

    try:
        response = session.get(url, proxies={"http": None, "https": None}, timeout=10)
        response.raise_for_status()
        imdata = response.json().get("imdata", [])

        # Build port → operSt map (parse from dn)
        port_map = {}
        for item in imdata:
            if api_class in item and "attributes" in item[api_class]:
                attrs = item[api_class]["attributes"]
                dn_val = attrs.get("dn", "")
                if "[" in dn_val and "]" in dn_val:
                    port = dn_val.split("[", 1)[1].split("]", 1)[0]
                    port_map[port] = attrs.get(check_key, "unknown")

        node_statuses = {}
        all_up = True
        for dn in dn_list:
            if "[" in dn and "]" in dn:
                port = dn.split("[", 1)[1].split("]", 1)[0]
                st = port_map.get(port, "unknown")
                node_statuses[port] = st
                if st != check_target:
                    all_up = False

        if not all_up:
            overall_all_up = False

        results.append(
            {
                "node_id": str(node_id),
                "hostname": hostname,
                "pod_id": str(pod_id),
                "checked_ports": len(node_statuses),
                f"{check_key}_statuses": node_statuses,
                "all_ports_up": all_up,
            }
        )

    except Exception as e:
        results.append(
            {
                "node_id": str(node_id),
                "hostname": hostname,
                "pod_id": str(pod_id),
                "checked_ports": 0,
                f"{check_key}_statuses": {},
                "all_ports_up": False,
                "error": f"{type(e).__name__}: {e}",
            }
        )
        overall_all_up = False

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if out_path is None:
        fname = f"leaf_oper_statuses_{hostname}_node{node_id}_pod{pod_id}_{ts}.json"
        out_path = os.path.join(os.getcwd(), fname)

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "apic": apic_ip,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "overall_all_ports_up": overall_all_up,
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception as e:
        print(f"{datetime.now()} Failed to write results file '{out_path}': {e}")

    return overall_all_up


def get_apic_admin_statuses(
    token,
    apic_ip,
    node_id,  # unused but kept for signature symmetry
    pod_id,
    hostname,
    dn_list,
    out_path=None,
    check_target="up",
):

    check_key = "adminSt"
    api_class = "l1PhysIf"

    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    # Collect unique node IDs from dn_list
    node_ids = []
    for line in dn_list:
        try:
            nid = line.split("/")[2].split("-")[1]
            if nid not in node_ids:
                node_ids.append(nid)
        except IndexError:
            continue

    results = []
    overall_all_up = True

    for nid in node_ids:
        node_dn_list = [
            line for line in dn_list if line.split("/")[2].split("-")[1] == nid
        ]
        url = f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{nid}/{api_class}.json"

        try:
            response = session.get(
                url, proxies={"http": None, "https": None}, timeout=10
            )
            response.raise_for_status()
            imdata = response.json().get("imdata", [])

            # Build port (ethX/Y) → adminSt map
            port_map = {
                item[api_class]["attributes"]["id"]: item[api_class]["attributes"].get(
                    check_key, "unknown"
                )
                for item in imdata
                if api_class in item and "attributes" in item[api_class]
            }

            node_statuses = {}
            all_up = True
            for dn in node_dn_list:
                if "[" in dn and "]" in dn:
                    port = dn.split("[", 1)[1].split("]", 1)[0]
                    st = port_map.get(port, "unknown")
                    node_statuses[port] = st
                    if st != check_target:
                        all_up = False

            if not all_up:
                overall_all_up = False

            results.append(
                {
                    "node_id": nid,
                    "pod_id": str(pod_id),
                    "checked_ports": len(node_statuses),
                    f"{check_key}_statuses": node_statuses,
                    "all_ports_up": all_up,
                }
            )

        except Exception as e:
            results.append(
                {
                    "node_id": nid,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "checked_ports": 0,
                    f"{check_key}_statuses": {},
                    "all_ports_up": False,
                    "error": f"{type(e).__name__}: {e}",
                }
            )
            overall_all_up = False

    # Write results to file
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if out_path is None:
        fname = f"apic_admin_statuses_{hostname}_pod{pod_id}_{ts}.json"
        out_path = os.path.join(os.getcwd(), fname)

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "apic": apic_ip,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "overall_all_ports_up": overall_all_up,
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception as e:
        print(f"{datetime.now()} Failed to write results file '{out_path}': {e}")

    return overall_all_up


def get_apic_oper_statuses(
    token,
    apic_ip,
    node_id,  # unused but kept for signature symmetry
    pod_id,
    hostname,
    dn_list,
    out_path=None,
    check_target="up",
):

    check_key = "operSt"
    api_class = "ethpmPhysIf"

    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    # Collect unique node IDs from dn_list
    node_ids = []
    for line in dn_list:
        try:
            nid = line.split("/")[2].split("-")[1]
            if nid not in node_ids:
                node_ids.append(nid)
        except IndexError:
            continue

    results = []
    overall_all_up = True

    for nid in node_ids:
        node_dn_list = [
            line for line in dn_list if line.split("/")[2].split("-")[1] == nid
        ]
        url = f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{nid}/{api_class}.json"

        try:
            response = session.get(
                url, proxies={"http": None, "https": None}, timeout=10
            )
            response.raise_for_status()
            imdata = response.json().get("imdata", [])

            # Build port (ethX/Y) → operSt map by parsing dn
            port_map = {}
            for item in imdata:
                if api_class in item and "attributes" in item[api_class]:
                    attrs = item[api_class]["attributes"]
                    dn_val = attrs.get("dn", "")
                    if "[" in dn_val and "]" in dn_val:
                        port = dn_val.split("[", 1)[1].split("]", 1)[0]
                        port_map[port] = attrs.get(check_key, "unknown")

            node_statuses = {}
            all_up = True
            for dn in node_dn_list:
                if "[" in dn and "]" in dn:
                    port = dn.split("[", 1)[1].split("]", 1)[0]
                    st = port_map.get(port, "unknown")
                    node_statuses[port] = st
                    if st != check_target:
                        all_up = False

            if not all_up:
                overall_all_up = False

            results.append(
                {
                    "node_id": nid,
                    "pod_id": str(pod_id),
                    "checked_ports": len(node_statuses),
                    f"{check_key}_statuses": node_statuses,
                    "all_ports_up": all_up,
                }
            )

        except Exception as e:
            results.append(
                {
                    "node_id": nid,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "checked_ports": 0,
                    f"{check_key}_statuses": {},
                    "all_ports_up": False,
                    "error": f"{type(e).__name__}: {e}",
                }
            )
            overall_all_up = False

    # Write results to file
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if out_path is None:
        fname = f"apic_oper_statuses_{hostname}_pod{pod_id}_{ts}.json"
        out_path = os.path.join(os.getcwd(), fname)

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "apic": apic_ip,
                    "hostname": hostname,
                    "pod_id": str(pod_id),
                    "overall_all_ports_up": overall_all_up,
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception as e:
        print(f"{datetime.now()} Failed to write results file '{out_path}': {e}")

    return overall_all_up


def compare_status_reports(
    file_before: str,
    file_after: str,
    ignore_top_fields: Optional[set] = None,
    allow_admin_oper_compare: bool = True,
    verbose: bool = True,
) -> bool:

    if ignore_top_fields is None:
        ignore_top_fields = {"timestamp", "overall_all_ports_up"}

    # all accepted per-node status-map keys
    STATUS_KEYS = (
        "adminSt_statuses",  # your files
        "admin_statuses",
        "operSt_statuses",
        "oper_statuses",
    )

    def _load(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _status_key(node_obj: Dict[str, Any]) -> str:
        for k in STATUS_KEYS:
            if k in node_obj and isinstance(node_obj[k], dict):
                return k
        raise KeyError("Could not find a status map key in node result")

    def _normalize_status_map(node_obj: Dict[str, Any]) -> Dict[str, str]:

        for k in STATUS_KEYS:
            if k in node_obj and isinstance(node_obj[k], dict):
                return node_obj[k]
        return {}

    def _index_by_node_id(results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out = {}
        for item in results:
            nid = str(item.get("node_id", "")).strip()
            if not nid:
                continue
            out[nid] = item
        return out

    def _compare_node_status_maps(
        node_id: str,
        before_node: Dict[str, Any],
        after_node: Dict[str, Any],
        diffs: List[str],
    ):
        key_before = _status_key(before_node)
        key_after = _status_key(after_node)

        # If admin vs oper comparison is NOT allowed and keys differ, flag it.
        if not allow_admin_oper_compare and key_before != key_after:
            diffs.append(
                f"[node {node_id}] status key differs: {key_before} vs {key_after}"
            )
            return

        bmap = _normalize_status_map(before_node)
        amap = _normalize_status_map(after_node)

        ports = set(bmap.keys()) | set(amap.keys())
        for p in sorted(ports):
            if p not in bmap:
                diffs.append(f"[node {node_id}] port added in after: {p} = {amap[p]}")
            elif p not in amap:
                diffs.append(
                    f"[node {node_id}] port missing in after: {p} (was {bmap[p]})"
                )
            else:
                if bmap[p] != amap[p]:
                    diffs.append(f"[node {node_id}] {p}: {bmap[p]} -> {amap[p]}")

    # ---- load & normalize ----
    try:
        before = _load(file_before)
        after = _load(file_after)
    except Exception as e:
        if verbose:
            print(f"Error loading files: {e}")
        return False

    # Ignore volatile top-level fields
    for k in ignore_top_fields:
        before.pop(k, None)
        after.pop(k, None)

    b_results = before.get("results", [])
    a_results = after.get("results", [])
    if not isinstance(b_results, list) or not isinstance(a_results, list):
        if verbose:
            print("Invalid format: 'results' must be a list in both files")
        return False

    b_index = _index_by_node_id(b_results)
    a_index = _index_by_node_id(a_results)

    diffs: List[str] = []

    # Node membership + per-node comparison
    for nid in sorted(set(b_index.keys()) | set(a_index.keys())):
        if nid not in b_index:
            diffs.append(f"[node {nid}] appears only in AFTER")
            continue
        if nid not in a_index:
            diffs.append(f"[node {nid}] missing in AFTER (was present in BEFORE)")
            continue

        # Optional: flag metadata changes (pod_id/hostname)
        b_meta = (
            str(b_index[nid].get("pod_id", "")),
            str(b_index[nid].get("hostname", "")),
        )
        a_meta = (
            str(a_index[nid].get("pod_id", "")),
            str(a_index[nid].get("hostname", "")),
        )
        if b_meta != a_meta:
            diffs.append(f"[node {nid}] metadata changed: {b_meta} -> {a_meta}")

        try:
            _compare_node_status_maps(nid, b_index[nid], a_index[nid], diffs)
        except Exception as e:
            diffs.append(f"[node {nid}] error comparing node: {type(e).__name__}: {e}")

    if verbose:
        if diffs:
            print("Differences found:")
            for d in diffs:
                print(" -", d)
        else:
            print("No differences found (per-node, per-port statuses identical).")

    return len(diffs) == 0


def wait_for_spine_status(
    hostname, desired_status=True, timeout=1800, interval=60
):
    start = time.time()
    while time.time() - start < timeout:
        try:
            new_token, new_apic_ip, _ = get_token_from_random_node(hostname)
            current = get_node_status(new_token, new_apic_ip, hostname)
            print(f"[DEBUG] Current status={current}, Desired={desired_status}")
            if current == desired_status:
                return True
        except Exception as e:
            print(f"[DEBUG] wait_for_spine_status retry: {type(e).__name__}: {e}")
        time.sleep(interval)
    return False


def get_spines(token, apic_ip):
    topSystem_url = f'{protocol}://{apic_ip}/api/node/class/topSystem.json?query-target-filter=eq(topSystem.role,"spine")'
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(topSystem_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        spine_dict = {}

        for i in imdata:
            name = i["topSystem"]["attributes"]["name"]
            oob_ip = i["topSystem"]["attributes"]["oobMgmtAddr"]
            inb_ip = i["topSystem"]["attributes"]["inbMgmtAddr"]
            if inb_ip != "0.0.0.0":
                spine_dict[name] = inb_ip
            else:
                spine_dict[name] = oob_ip

        return spine_dict
    except Exception as e:
        print(f"{timestamp()} Failed to get spines. {e}")
        return None


def get_leaf(token, apic_ip, hostname):
    topSystem_url = f'{protocol}://{apic_ip}/api/node/class/topSystem.json?query-target-filter=eq(topSystem.name,"{hostname}")'
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(topSystem_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        leaf_dict = {}

        for i in imdata:
            name = i["topSystem"]["attributes"]["name"]
            oob_ip = i["topSystem"]["attributes"]["oobMgmtAddr"]
            inb_ip = i["topSystem"]["attributes"]["inbMgmtAddr"]
            if inb_ip != "0.0.0.0":
                leaf_dict[name] = inb_ip
            else:
                leaf_dict[name] = oob_ip

        print(leaf_dict)
        return leaf_dict
    except Exception as e:
        print(f"{timestamp()} Failed to get leaf. {e}")
        return None


def get_apic_leaf_hostname(token, apic_ip, node_id):
    topSystem_url = f'{protocol}://{apic_ip}/api/node/class/topSystem.json?query-target-filter=eq(topSystem.id,"{node_id}")'
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(topSystem_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        for i in imdata:
            name = i["topSystem"]["attributes"]["name"]

        print(name)
        return name
    except Exception as e:
        print(f"{timestamp()} Failed to get apic leaf hostname. {e}")
        return None


def check_only_neighbor(output, target_device):
    lines = output.strip().split("\n")
    device_ids = []

    # ヘッダー行の次から処理
    for line in lines[1:]:
        parts = line.split()
        if parts:
            device_ids.append(parts[0])

    # 指定したデバイスだけが存在しているか
    return all(did == target_device for did in device_ids) if device_ids else False


def check_only_neighbor_from_file(file_path, target_device):
    with open(file_path, "r") as file:
        output = file.read()
    return check_only_neighbor(output, target_device)


def get_node_status(token, apic_ip, hostname):
    fabricNode_url = f'{protocol}://{apic_ip}/api/node/class/fabricNode.json?query-target-filter=eq(fabricNode.name,"{hostname}")'
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(fabricNode_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        if not imdata:
            return False

        fabricSt = imdata[0]["fabricNode"]["attributes"]["fabricSt"]

        return fabricSt.lower() == "active"

    except Exception as e:
        print(f"{timestamp()} Failed to get apic leaf. {e}")
        return False


def refresh_token(token, apic_ip):
    refresh_url = f"{protocol}://{apic_ip}/api/node/class/aaaRefresh.json"
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(refresh_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        if not imdata:
            return False

        new_token = imdata[0]["aaaLogin"]["attributes"]["token"]
        return new_token

    except Exception as e:
        print(f"{timestamp()} Failed to refresh token. {e}")
        return False


def get_spine_ports(token, apic_ip, node_id, pod_id, apic_leaf):
    lldpIf_url = f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{node_id}/lldpIf.json?rsp-subtree=children&rsp-subtree-class=lldpIf,lldpAdjEp&rsp-subtree-include=required"
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    try:
        response = session.get(lldpIf_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        dn_list = []

        for i in imdata:
            parent = list(i.values())[0]
            parent_dn = parent.get("attributes", {}).get("dn", "")

            for child in parent.get("children", []):
                if "lldpAdjEp" in child:
                    attrs = child["lldpAdjEp"].get("attributes", {})
                    sysdesc = attrs.get("sysDesc", "")
                    portidv = attrs.get("portIdV", "")

                    if not "topology/pod-" in sysdesc.lower():
                        continue

                    transformed = f"{sysdesc}/pathep-[{portidv}]".replace(
                        "node", "paths"
                    ).replace("Eth", "eth")
                    dn_list.append(transformed)

        return dn_list

    except Exception as e:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{now} Failed to get spine ports. {e}")
        return None


def transform_string(s):
    s = re.sub(r"node-", "paths-", s)
    s = re.sub(r"sys/phys-", "pathep-", s)
    s = s.replace("/phys", "")
    return s


def replace_characters(file, find="", replacement=r""):
    with open(file, "r", encoding="utf-8") as f:
        content = f.read()

    last_comma_index = content.rfind(find)
    if last_comma_index != -1:
        content = (
            content[:last_comma_index] + replacement + content[last_comma_index + 1 :]
        )

    with open(file, "w", encoding="utf-8") as f:
        f.write(content)


def extract_key(port):
    match = re.search(r"pod-(\d+)/paths-(\d+)", port)
    if match:
        pod_number = int(match.group(1))
        path_number = int(match.group(2))
    else:
        pod_number = float("inf")
        path_number = float("inf")

    return (pod_number, path_number)


def split_file_by_lines(path, filename, lines_per_file):
    infile_path = os.path.join(path, filename)
    with open(infile_path, "r") as infile:
        filtered_lines = [
            line.replace("]}}]}}]}}", ",") for line in infile if "pathep" in line
        ]

    total_parts = (len(filtered_lines) + lines_per_file - 1) // lines_per_file
    digits = max(2, len(str(total_parts)))

    noshut_part_files = []

    part = 1
    for i in range(0, len(filtered_lines), lines_per_file):
        chunk = filtered_lines[i : i + lines_per_file]
        part_str = str(part).zfill(digits)
        output_filename = os.path.join(path, f"{part_str}_part_{filename}")

        with open(output_filename, "w") as f:
            f.write(
                '{"polUni":{"attributes":{"dn":"uni"},"children":[\n{"fabricInst":{"attributes":{"dn":"uni/fabric"},"children":[\n{"fabricOOServicePol":{"attributes":{"dn":"uni/fabric/outofsvc"},"children":[\n'
            )
            f.writelines(chunk)

        with open(output_filename, "r") as f:
            content = f.read()
        last_comma_index = content.rfind(",")
        if last_comma_index != -1:
            content = (
                content[:last_comma_index]
                + "]}}]}}]}}"
                + content[last_comma_index + 1 :]
            )
        with open(output_filename, "w") as f:
            f.write(content)

        noshut_part_files.append(output_filename)
        part += 1

    return noshut_part_files


def get_logs(hostname, ip, username, password, command_file, output_file):

    with open(command_file, "r") as f:
        commands = [line.strip() for line in f if line.strip()]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(
            hostname=ip, username=username, password=password, look_for_keys=False
        )

        with open(output_file, "w", encoding="utf-8") as f:
            for cmd in commands:
                f.write(f"===== {cmd} =====\n")
                stdin, stdout, stderr = ssh.exec_command(cmd)
                output = stdout.read().decode()
                error = stderr.read().decode()
                f.write(output if output else error)
                f.write("\n\n")

        print(f"{timestamp()} [+] Output written to {output_file}")

    except Exception as e:
        print(f"{timestamp()} [!] SSH error on {hostname} ({ip}): {e}")
        raise
    finally:
        ssh.close()


def get_check_logs(hostname, ip, username, password, command, output_file):

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(
            hostname=ip, username=username, password=password, look_for_keys=False
        )

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"===== {command} =====\n")
            stdin, stdout, stderr = ssh.exec_command(command)
            output = stdout.read().decode()
            error = stderr.read().decode()
            f.write(output if output else error)

        print(f"{timestamp()} [+] Output written to {output_file}")

    except Exception as e:
        print(f"{timestamp()} [!] SSH error on {hostname} ({ip}): {e}")
        raise
    finally:
        ssh.close()


def compare_ssh_logs(
    file_before: str,
    file_after: str,
    diff_out: str,
    log_directory: str,
    uid: str,
    hostname: str,
) -> bool:

    try:
        with open(file_before, "r", encoding="utf-8", errors="ignore") as f1, open(
            file_after, "r", encoding="utf-8", errors="ignore"
        ) as f2:
            before_lines = f1.readlines()
            after_lines = f2.readlines()
    except Exception as e:
        log_processing(
            log_directory, uid, f"{hostname}: SSHログ比較失敗 (読み込みエラー)"
        )
        log_detail(
            log_directory,
            uid,
            f"{hostname}: SSH log read error {type(e).__name__}: {e}",
        )
        return False

    if before_lines == after_lines:
        log_processing(log_directory, uid, f"{hostname}: SSHログ比較 -> 差分なし (OK)")
        return True

    # Differences found → generate diff
    diff = difflib.unified_diff(
        before_lines,
        after_lines,
        fromfile=os.path.basename(file_before),
        tofile=os.path.basename(file_after),
        lineterm="",
    )
    print(diff)

    # 差分の行だけを抽出
    diff_lines = [line for line in diff if line.startswith("+") or line.startswith("-")]
    print(diff_lines)

    try:
        with open(diff_out, "w", encoding="utf-8") as f:
            for line in diff_lines:
                f.write(line + "\n")
        log_processing(log_directory, uid, f"{hostname}: SSHログ比較 -> 差分あり (NG)")
        log_detail(log_directory, uid, f"{hostname}: diffファイル出力 -> {diff_out}")
    except Exception as e:
        log_processing(log_directory, uid, f"{hostname}: SSHログ比較 -> 差分出力失敗")
        log_detail(
            log_directory,
            uid,
            f"{hostname}: diffファイル書き込みエラー {type(e).__name__}: {e}",
        )

    return False


### FOR TEST ###
"""def post_file(
    token,
    apic_ip,
    node_id,
    pod_id,
    file_path,
    log_directory=None,
    uid=None,
    retries=3,
    backoff=5,
):
    # Change later
    print(f"Posting file: {file_path}")
    print(f"{apic_ip}, {node_id}, {pod_id}, {log_directory}, {uid}")
    return True"""


### FOR TEST ###

def post_file(token, apic_ip, node_id, pod_id, file_path,
              log_directory=None, uid=None,
              retries=3, backoff=5):

    url = f"{protocol}://{apic_ip}/api/mo/uni.json"
    session = requests.Session()
    session.verify = False
    session.headers.update({
        'Cookie': f'APIC-Cookie={token}',
        'Content-Type': 'application/json'
    })

    for attempt in range(1, retries + 1):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                payload = json.load(f)

            resp = session.post(url, json=payload, proxies={"http": None, "https": None}, timeout=20)
            resp.raise_for_status()
            log_processing(log_directory, uid, f"POST成功 {file_path}")
            return resp.json()

        except Exception as e:
            log_processing(log_directory, uid, f"POST失敗 {file_path} (試行{attempt}/{retries})")
            log_detail(log_directory, uid, f"{file_path}: {type(e).__name__}: {e}")

            if attempt < retries:
                time.sleep(backoff)
                continue
            else:
                log_processing(log_directory, uid, f"POST最終失敗 {file_path}")
                return False


### FOR TEST ###
"""def reload_node(
    token, apic_ip, node_id, pod_id, log_directory=None, uid=None, retries=3, backoff=5
):
    # Change later
    print("Reloading Node")
    print(f"{apic_ip}, {node_id}, {pod_id}, {log_directory}, {uid}")
    return True"""


### FOR TEST ###


def reload_node(token, apic_ip, node_id, pod_id,
                log_directory=None, uid=None,
                retries=3, backoff=5):

    # URL you specified:
    # /api/node/mo/topology/pod-1/node-101/sys/action.json
    url = f"{protocol}://{apic_ip}/api/node/mo/topology/pod-{pod_id}/node-{node_id}/sys/action.json"

    session = requests.Session()
    session.verify = False
    session.headers.update({
        'Cookie': f'APIC-Cookie={token}',
        'Content-Type': 'application/json'
    })

    # Build DNs dynamically
    dn_ch    = f"topology/pod-{pod_id}/node-{node_id}/sys/ch"
    dn_lsubj = f"topology/pod-{pod_id}/node-{node_id}/sys/action/lsubj-[{dn_ch}]"
    dn_task  = f"{dn_lsubj}/eqptChReloadLTask"

    payload = {
        "actionLSubj": {
            "attributes": {
                "dn": dn_lsubj,
                "oDn": dn_ch
            },
            "children": [
                {
                    "eqptChReloadLTask": {
                        "attributes": {
                            "dn": dn_task,
                            "adminSt": "start"
                        },
                        "children": []
                    }
                }
            ]
        }
    }

    for attempt in range(1, retries + 1):
        try:
            resp = session.post(
                url,
                json=payload,
                proxies={"http": None, "https": None},
                timeout=20
            )
            # Optional: log response body for debug
            # log_detail(log_directory, uid, f"reload_node response {resp.status_code}: {resp.text}")

            resp.raise_for_status()
            log_processing(log_directory, uid,
                           f"reload_node 成功 pod-{pod_id}/node-{node_id}")
            return True

        except Exception as e:
            log_processing(
                log_directory,
                uid,
                f"reload_node 失敗 pod-{pod_id}/node-{node_id} (試行{attempt}/{retries})"
            )

            # Try to include response body if available
            try:
                body = resp.text
            except Exception:
                body = "NO RESPONSE"

            log_detail(
                log_directory,
                uid,
                f"reload_node error {type(e).__name__}: {e} | body: {body}"
            )

            if attempt < retries:
                time.sleep(backoff)
                continue
            else:
                log_processing(
                    log_directory,
                    uid,
                    f"reload_node 最終失敗 pod-{pod_id}/node-{node_id}"
                )
                return False


def comma_separated_list(value):
    # return [x.strip() for x in value.split(",") if x.strip()]
    return [x.strip() for x in re.split(r"[,\s]+", value) if x.strip()]


def log_step(log_path, message):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{timestamp()} {message}\n")


def main():
    # ==== 入口処理: 引数を受け取り、最低限のバリデーションを実施 ====
    parser = argparse.ArgumentParser(description="nodeshut_vup tool")
    parser.add_argument("--target_nodes", type=comma_separated_list)
    parser.add_argument("--pid", help="PID")
    parser.add_argument("--scenario_id", help="enable or disable")
    parser.add_argument("--type", help="leaf or spine")
    parser.add_argument("--order_group", help="order group")
    args = parser.parse_args()

    hostnames = args.target_nodes or []
    uid = args.order_group  # or str(uuid.uuid4())
    scenario = args.scenario_id
    node_type = args.type
    pid = args.pid  # or str(uuid.uuid4())

    commands_directory = f"{script_directory}/commands/"

    log_directory = f"{script_directory}/log/{uid}"
    scenario_directory = f"{log_directory}/{scenario}"

    status_path = f"{log_directory}/{pid}_status.json"
    main_log_path = f"{log_directory}/{pid}.log"
    processing_log = f"{log_directory}/{pid}_processing.log"
    detail_log = f"{log_directory}/{pid}_detail.log"

    errors = []

    if not hostnames:
        errors.append("target_nodes が指定されていません。")

    if scenario not in ("enable", "disable"):
        errors.append("scenario_id が不明または指定されていません。")

    if node_type not in ("leaf", "spine"):
        errors.append("type が不正または未指定です。")

    if not uid:
        errors.append("order_group が指定されていません。")

    if not pid:
        errors.append("PID が指定されていません。")

    if hostnames and node_type in ("leaf", "spine"):
        """if not all(
            ("Leaf" in h if node_type == "leaf" else "SpSw" in h) for h in hostnames
        ):"""
        if not all(
            (
                ("Leaf" in h or "leaf" in h) if node_type == "leaf" else "SpSw" in h
            ) for h in hostnames
        ):
            errors.append("Node type と hostname の命名規制が一致しません。")

    if node_type == "spine" and len(hostnames) != 1:
        errors.append("type=spine の場合、target_nodes は1ノードのみ指定してください。")

    if errors:
        msg = " / ".join(errors)
        print(f"{timestamp()} {msg}")
        set_client_error_status(
            log_directory,
            pid,
            hostnames,
            msg,
            code=result_code.STATUS_CODE_CLIENT_ERROR,
        )
        sys.exit(1)

    if scenario == "disable":
        """if os.path.exists(log_directory):
            msg = f"指定された order_group '{uid}' は既に存在しています。"
            print(f"{timestamp()} {msg}")
            set_client_error_status(
                log_directory,
                pid,
                hostnames,
                msg,
                code=result_code.DUPLICATE_ID_CLIENT_ERROR,
            )
            sys.exit(1)"""

        os.makedirs(log_directory, exist_ok=True)
        os.makedirs(scenario_directory, exist_ok=True)

    elif scenario == "enable":
        if not os.path.exists(log_directory):
            msg = f"指定された order_group '{uid}' が存在しません。"
            print(f"{timestamp()} {msg}")
            set_client_error_status(
                log_directory,
                pid,
                hostnames,
                msg,
                code=result_code.DUPLICATE_ID_CLIENT_ERROR,
            )
            sys.exit(1)

        os.makedirs(scenario_directory, exist_ok=True)

    if any(
        os.path.exists(p)
        for p in [status_path, main_log_path, processing_log, detail_log]
    ):
        msg = f"PID '{pid}' のステータス／ログファイルがすでに存在しています。"
        print(f"{timestamp()} {msg}")
        set_client_error_status(
            log_directory,
            pid,
            hostnames,
            msg,
            code=result_code.DUPLICATE_ID_CLIENT_ERROR,
        )
        sys.exit(1)

    json_nodes = [
        {
            "target_node": hostname,
            "each_status_code": result_code.EACH_STATUS_CODE_IN_PROGRESS,
            "message": f"{hostname}の処理中",
        }
        for hostname in (hostnames or [])
    ]

    json_data_structure = {
        "status_code": result_code.STATUS_CODE_SUCCESS,
        "message": "処理中",
        "results": json_nodes,
    }

    with open(f"{log_directory}/{pid}_status.json", "w") as f:
        json.dump(json_data_structure, f, ensure_ascii=False, indent=4)

    for name in ["processing", "detail"]:
        open(f"{log_directory}/{pid}_{name}.log", "w").close()

    log_processing(
        log_directory,
        pid,
        f"ログ初期化: {log_directory}/{pid}_processing.log, {log_directory}/{pid}_detail.log",
    )

    valid_hosts = []
    for h in hostnames:
        if hostname_exists(h):
            valid_hosts.append(h)

    if not valid_hosts:
        msg = "指定された hostname が許可リストに含まれていません。"
        print(f"{timestamp()} {msg}")
        log_processing(log_directory, pid, msg)
        set_client_error_status(
            log_directory,
            pid,
            hostnames,
            msg,
            code=result_code.HOSTNAME_NOT_ALLOWED_CLIENT_ERROR,
        )
        sys.exit(1)

    if len(valid_hosts) != len(hostnames):
        missing = [h for h in hostnames if h not in valid_hosts]
        msg = "存在しない hostname があります: " + ", ".join(missing)
        print(f"{timestamp()} {msg}")
        log_processing(log_directory, pid, msg)
        set_client_error_status(
            log_directory,
            pid,
            hostnames,
            msg,
            code=result_code.HOSTNAME_NOT_ALLOWED_CLIENT_ERROR,
        )
        sys.exit(1)

    hostnames = valid_hosts

    step = 1
    steps = 3 if node_type == "leaf" else 4
    action = "ノード切り離し" if scenario == "disable" else "ノード組み込み"

    # ==== STEP1: APIC への接続確認 ====
    log_step(main_log_path, f"{action}:{node_type.capitalize()} START")
    log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 START")

    try:
        token_node = random.choice(valid_hosts)
        log_processing(log_directory, pid, f"APIC接続試行: token_node={token_node}")
        token, apic_ip, apic = get_token_from_random_node(token_node)
        log_processing(
            log_directory, pid, f"APIC接続成功: apic_ip={apic_ip}, apic={apic}"
        )
    except Exception as e:
        log_processing(log_directory, pid, "APIC接続失敗")
        log_detail(
            log_directory,
            pid,
            f"APIC接続例外: {type(e).__name__}: {e}\n{traceback.format_exc()}",
        )
        print(f"{timestamp()} Failed to retrieve APIC token or IP.")
        log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 ERROR")
        log_step(main_log_path, f"{action}:{node_type.capitalize()} ERROR")
        fail_all_and_exit(log_directory, pid, hostnames, "APIC接続失敗")

    log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 END")
    step = step + 1

    if scenario == "disable":

        for hostname in hostnames:
            try:

                # ==== STEP2: shutdown 投入前の準備（ID取得、状態採取、shutdown投入）====
                log_processing(
                    log_directory,
                    pid,
                    f"{hostname}: 処理開始 (scenario=disable, type={node_type})",
                )

                try:
                    token_node = random.choice(valid_hosts)
                    log_processing(log_directory, pid, f"APIC接続試行: token_node={hostname}")
                    token, apic_ip, apic = get_token_from_random_node(hostname)
                    log_processing(
                        log_directory, pid, f"APIC接続成功: apic_ip={apic_ip}, apic={apic}"
                    )
                except Exception as e:
                    log_processing(log_directory, pid, "APIC接続失敗")
                    log_detail(
                        log_directory,
                        pid,
                        f"APIC接続例外: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                    )
                    print(f"{timestamp()} Failed to retrieve APIC token or IP.")
                    log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 ERROR")
                    log_step(main_log_path, f"{action}:{node_type.capitalize()} ERROR")
                    update_node_status(
                        log_directory,
                        pid,
                        hostname,
                        result_code.EACH_STATUS_CODE_SERVER_ERROR,
                        f"{hostname}: APIC接続失敗",
                    )
                    continue

                node_id, pod_id = get_hostname_info(hostname, apic_ip, apic, token)

                if not node_id or not pod_id:
                    log_step(main_log_path, f"[STEP{step}/{steps}]ノード切り離し ERROR")
                    log_step(
                        main_log_path, f"ノード切り離し:{node_type.capitalize()} ERROR"
                    )
                    log_processing(
                        log_directory, pid, f"{hostname}: node_id/pod_id 取得失敗"
                    )
                    log_detail(
                        log_directory,
                        pid,
                        f"{hostname}: get_hostname_info 失敗 apic_ip={apic_ip}, apic={apic}",
                    )
                    raise RuntimeError("node_id / pod_id が取得できません")
                else:
                    log_detail(
                        log_directory,
                        pid,
                        f"{hostname}: node_id={node_id}, pod_id={pod_id}",
                    )

                if node_type == "leaf":

                    shutdown_file, dn_list, spine_dn_list = create_leaf_shutdown(
                        token, apic_ip, hostname, node_id, pod_id, uid
                    )
                    log_processing(
                        log_directory, pid, f"{hostname}: shutdown/noshut 生成"
                    )
                    log_detail(
                        log_directory,
                        pid,
                        f"{hostname}: shutdown_file={shutdown_file}, dn_count={len(dn_list)}, spine_dn_count={len(spine_dn_list)}",
                    )


                    spine_admin_status_list = get_leaf_admin_statuses(
                        token,
                        apic_ip,
                        node_id,
                        pod_id,
                        hostname,
                        spine_dn_list,
                        f"{scenario_directory}/{hostname}_{scenario}_before_spine_admin_statuses.json",
                        check_target="up",
                    )
                    spine_oper_status_list = get_leaf_oper_statuses(
                        token,
                        apic_ip,
                        node_id,
                        pod_id,
                        hostname,
                        spine_dn_list,
                        f"{scenario_directory}/{hostname}_{scenario}_before_spine_oper_statuses.json",
                        check_target="up",
                    )


                    apic_dn_list = get_apic_ports(token, apic_ip, node_id, pod_id, up_only=False)
                    apic_connected = any(f"node-{node_id}/" in dn for dn in apic_dn_list)

                    if apic_connected:
                        apic_admin_status_list = get_apic_admin_statuses(
                            token, apic_ip, node_id, pod_id, hostname,
                            apic_dn_list,
                            f"{scenario_directory}/{hostname}_{scenario}_before_apic_admin_statuses.json",
                            check_target="up",
                        )
                        apic_oper_status_list = get_apic_oper_statuses(
                            token, apic_ip, node_id, pod_id, hostname,
                            apic_dn_list,
                            f"{scenario_directory}/{hostname}_{scenario}_before_apic_oper_statuses.json",
                            check_target="up",
                        )
                        log_processing(log_directory, pid, f"{hostname}: APIC向けポート状態採取完了（leaf）")
                    else:
                        log_processing(log_directory, pid, f"{hostname}: APICと非接続のためAPIC向けポート採取スキップ")
                    log_detail(
                        log_directory,
                        pid,
                        f"{hostname}: before_spine_admin={log_directory}/{hostname}_{scenario}_before_spine_admin_statuses.json, "
                        f"before_spine_oper={log_directory}/{hostname}_{scenario}_before_spine_oper_statuses.json"
                        f"before_apic_admin={log_directory}/{hostname}_{scenario}_before_apic_admin_statuses.json, "
                        f"before_apic_oper={log_directory}/{hostname}_{scenario}_before_apic_oper_statuses.json",
                    )


                    if shutdown_file:

                        admin_status_list = get_leaf_admin_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            dn_list,
                            f"{scenario_directory}/{hostname}_{scenario}_before_admin_statuses.json",
                            check_target="up",
                        )
                        oper_status_list = get_leaf_oper_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            dn_list,
                            f"{scenario_directory}/{hostname}_{scenario}_before_oper_statuses.json",
                            check_target="up",
                        )
                       


                        log_processing(
                            log_directory,
                            pid,
                            f"{hostname}: before 状態採取完了（leaf）",
                        )
                        log_detail(
                            log_directory,
                            pid,
                            f"{hostname}: before_admin={log_directory}/{hostname}_{scenario}_before_admin_statuses.json, "
                            f"before_oper={log_directory}/{hostname}_{scenario}_before_oper_statuses.json",
                        )

                        log_step(
                            main_log_path,
                            f"[STEP2/{steps}] {hostname} ノード切り離し START",
                        )
                        log_processing(
                            log_directory, pid, f"{hostname}: shutdown投入開始"
                        )
                        log_detail(
                            log_directory,
                            pid,
                            f"{hostname}: POSTファイル={shutdown_file}, dn_count={len(dn_list)}, spine_dn_count={len(spine_dn_list)}",
                        )

                        resp = post_file(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            shutdown_file,
                            log_directory=log_directory,
                            uid=pid,
                        )

                        if resp is False:
                            log_step(
                                main_log_path,
                                f"[STEP{step}/{steps}]ノード切り離し ERROR",
                            )
                            log_step(
                                main_log_path,
                                f"ノード切り離し:{node_type.capitalize()} ERROR",
                            )
                            update_node_status(
                                log_directory,
                                pid,
                                hostname,
                                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                f"{hostname}: POST失敗: {shutdown_file}",
                            )
                            continue

                        log_processing(
                            log_directory, pid, f"{hostname}: shutdown投入完了"
                        )

                        time.sleep(5)

                        try:
                            log_processing(log_directory, pid, f"APIC接続試行（shutdown後）: token_node={hostname}")
                            token, apic_ip, apic = get_token_from_random_node(hostname)
                            log_processing(
                                log_directory, pid, f"APIC接続成功: apic_ip={apic_ip}, apic={apic}"
                            )
                        except Exception as e:
                            log_processing(log_directory, pid, "APIC接続失敗")
                            log_detail(
                                log_directory,
                                pid,
                                f"APIC接続例外: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                            )
                            log_step(main_log_path, f"[STEP{step}/{steps}]ノード切り離し ERROR")
                            log_step(main_log_path, f"{action}:{node_type.capitalize()} ERROR")
                            raise RuntimeError("shutdown後のAPIC接続失敗")
                        
                        time.sleep(5)

                        admin_status_list = get_leaf_admin_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            dn_list,
                            f"{scenario_directory}/{hostname}_{scenario}_after_admin_statuses.json",
                            check_target="down",
                        )
                        oper_status_list = get_leaf_oper_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            dn_list,
                            f"{scenario_directory}/{hostname}_{scenario}_after_oper_statuses.json",
                            check_target="down",
                        )
                        spine_admin_status_list = get_leaf_admin_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            spine_dn_list,
                            f"{scenario_directory}/{hostname}_{scenario}_after_spine_admin_statuses.json",
                            check_target="up",
                        )
                        spine_oper_status_list = get_leaf_oper_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            spine_dn_list,
                            f"{scenario_directory}/{hostname}_{scenario}_after_spine_oper_statuses.json",
                            check_target="up",
                        )
                        log_processing(
                            log_directory,
                            pid,
                            f"{hostname}: after 状態採取完了（leaf）",
                        )
                        log_detail(
                            log_directory,
                            pid,
                            f"{hostname}: after_admin={log_directory}/{hostname}_{scenario}_after_admin_statuses.json, "
                            f"after_oper={log_directory}/{hostname}_{scenario}_after_oper_statuses.json, "
                            f"after_spine_admin={log_directory}/{hostname}_{scenario}_after_spine_admin_statuses.json, "
                            f"after_spine_oper={log_directory}/{hostname}_{scenario}_after_spine_oper_statuses.json",
                        )

                        if (
                            not admin_status_list
                            or not spine_admin_status_list
                            or not spine_oper_status_list
                        ):
                            log_step(
                                main_log_path,
                                f"[STEP{step}/{steps}]ノード切り離し ERROR",
                            )
                            log_step(
                                main_log_path,
                                f"ノード切り離し:{node_type.capitalize()} ERROR",
                            )
                            log_processing(
                                log_directory,
                                pid,
                                f"{hostname}: LeafポートStatus想定外あり",
                            )
                            raise RuntimeError("LeafポートStatus想定外あり")

                    # Correct: if admin_status_list:
                    leaf_dict = get_leaf(token, apic_ip, hostname)
                    leaf = leaf_dict[hostname]
                    commands_file = f"{commands_directory}leaf_commands.txt"

                    log_processing(
                        log_directory, pid, f"{hostname}: SSHログ採取開始（leaf）"
                    )
                    log_detail(
                        log_directory, pid, f"{hostname}: commands_file={commands_file}"
                    )

                    get_logs(
                        hostname,
                        leaf,
                        apic_username,
                        apic_password,
                        commands_file,
                        f"{scenario_directory}/{log_timestamp()}_{hostname}.log",
                    )

                    log_processing(
                        log_directory, pid, f"{hostname}: SSHログ採取完了（leaf）"
                    )
                    log_step(
                        main_log_path, f"[STEP2/{steps}] {hostname} ノード切り離し END"
                    )
                    update_node_status(
                        log_directory,
                        pid,
                        hostname,
                        result_code.EACH_STATUS_CODE_COMPLETED,
                        f"{hostname}の切り離し正常終了",
                    )

                else:

                    # spine の場合: 複数 spine の SSH ログ採取と APIC 片寄（shutdown）を実施
                    spines = get_spines(token, apic_ip)
                    print(spines)

                    commands_file = f"{commands_directory}all_spine_commands.txt"

                    exceptions = []

                    def get_logs_wrapper(*args, **kwargs):
                        try:
                            get_logs(*args, **kwargs)
                        except Exception as e:
                            exceptions.append(e)

                    threads = []

                    for spine, ip in spines.items():
                        log_processing(
                            log_directory,
                            pid,
                            f"{spine}: {commands_file} SSHログ採取開始（spine）",
                        )
                        t = threading.Thread(
                            target=get_logs_wrapper,
                            args=(
                                spine,
                                ip,
                                apic_username,
                                apic_password,
                                commands_file,
                                f"{scenario_directory}/{log_timestamp()}_{spine}_{scenario}_before_all_spine.log",
                            ),
                        )
                        t.start()
                        threads.append(t)

                    for t in threads:
                        t.join()

                    if exceptions:
                        for e in exceptions:
                            print(f"例外発生: {e}")
                            log_processing(log_directory, pid, f"例外発生: {e}")

                        log_processing(log_directory, pid, "SSHログ採取失敗")
                        log_detail(
                            log_directory,
                            pid,
                            f"SSHログ採取失敗: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                        )
                        print(f"{timestamp()} Failed to get SSH logs.")
                        log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 ERROR")
                        log_step(
                            main_log_path, f"{action}:{node_type.capitalize()} ERROR"
                        )
                        update_node_status(
                            log_directory,
                            pid,
                            hostname,
                            result_code.EACH_STATUS_CODE_SERVER_ERROR,
                            f"{hostname}: SSHログ採取失敗",
                        )
                        continue

                    log_processing(
                        log_directory,
                        pid,
                        f"{spine}: {commands_file} SSHログ採取完了（spine）",
                    )

                    check_commands_file = f"{commands_directory}check_commands.txt"

                    with open(check_commands_file, "r") as f:
                        lines = f.readlines()

                    exceptions = []

                    def get_check_logs_wrapper(*args, **kwargs):
                        try:
                            get_check_logs(*args, **kwargs)
                        except Exception as e:
                            exceptions.append(e)

                    threads = []

                    for line in lines:
                        words = line.strip().split()
                        threads = []  # ループごとにリストを初期化
                        for spine, ip in spines.items():
                            log_processing(
                                log_directory,
                                pid,
                                f"{spine}: {words[1]} SSHログ採取開始（spine）",
                            )
                            t = threading.Thread(
                                target=get_check_logs_wrapper,
                                args=(
                                    spine,
                                    ip,
                                    apic_username,
                                    apic_password,
                                    line.strip(),
                                    f"{scenario_directory}/{spine}_{scenario}_before_{words[1]}.log",
                                ),
                            )
                            t.start()
                            threads.append((t, spine))

                        for t, spine in threads:
                            t.join()

                    if exceptions:
                        for e in exceptions:
                            print(f"例外発生: {e}")
                            log_processing(log_directory, pid, f"例外発生: {e}")

                        log_processing(log_directory, pid, "SSHログ採取失敗")
                        log_detail(
                            log_directory,
                            pid,
                            f"SSHログ採取失敗: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                        )
                        print(f"{timestamp()} Failed to get SSH logs.")
                        log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 ERROR")
                        log_step(
                            main_log_path, f"{action}:{node_type.capitalize()} ERROR"
                        )
                        update_node_status(
                            log_directory,
                            pid,
                            hostname,
                            result_code.EACH_STATUS_CODE_SERVER_ERROR,
                            f"{hostname}: SSHログ採取失敗",
                        )
                        continue

                    log_processing(
                        log_directory,
                        pid,
                        f"{spine}: {check_commands_file} SSHログ採取完了（spine）",
                    )

                    log_processing(
                        log_directory, pid, f"{hostname}: Module 確認開始（spine）"
                    )
                    modules_ok, modules_msg = analyze_show_module_log(
                        f"{scenario_directory}/{hostname}_{scenario}_before_module.log"
                    )

                    if not modules_ok:
                        log_processing(
                            log_directory, pid, f"{hostname}: Module 確認失敗"
                        )
                        log_detail(
                            log_directory,
                            pid,
                            f"{hostname}: Module 確認失敗: {modules_msg}",
                        )
                        print(f"{timestamp()} Module 確認失敗: {modules_msg}")
                        log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 ERROR")
                        log_step(
                            main_log_path, f"{action}:{node_type.capitalize()} ERROR"
                        )
                        update_node_status(
                            log_directory,
                            pid,
                            hostname,
                            result_code.EACH_STATUS_CODE_SERVER_ERROR,
                            f"{hostname}: Module 確認失敗: {modules_msg}",
                        )
                        continue

                    log_processing(
                        log_directory, pid, f"{hostname}: Diagnostic 確認開始（spine）"
                    )
                    diag_ok, diag_msg = analyze_diag_result_log(
                        f"{scenario_directory}/{hostname}_{scenario}_before_diagnostic.log"
                    )

                    if not diag_ok:
                        log_processing(
                            log_directory, pid, f"{hostname}: DIAG TEST 失敗"
                        )
                        log_detail(
                            log_directory,
                            pid,
                            f"{hostname}: DIAG TEST 失敗: {diag_msg}",
                        )
                        print(f"{timestamp()} DIAG TEST 失敗: {diag_msg}")
                        log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 ERROR")
                        log_step(
                            main_log_path, f"{action}:{node_type.capitalize()} ERROR"
                        )
                        update_node_status(
                            log_directory,
                            pid,
                            hostname,
                            result_code.EACH_STATUS_CODE_SERVER_ERROR,
                            f"{hostname}: DIAG TEST 失敗: {diag_msg}",
                        )
                        continue

                    log_step(main_log_path, f"[STEP{step}/{steps}]APIC片寄 START")
                    (
                        apic_shutdown_file,
                        apic_dn_list,
                        apic_leaf_dn_list,
                        other_apic_leaf_dn_list,
                    ) = create_apic_shutdown(
                        token, apic_ip, hostname, node_id, pod_id, uid
                    )
                    log_processing(log_directory, pid, f"APIC: shutdown/noshut 生成")
                    log_detail(
                        log_directory,
                        pid,
                        f"{hostname}: shutdown_file={apic_shutdown_file}, dn_count={len(other_apic_leaf_dn_list)}",
                    )

                    apic_oper_status_list = get_apic_oper_statuses(
                        token,
                        apic_ip,
                        node_id,
                        pod_id,
                        hostname,
                        apic_dn_list,
                        f"{scenario_directory}/apic_{scenario}_before_oper_statuses.json",
                        check_target="up",
                    )
                    log_processing(
                        log_directory,
                        pid,
                        f"{hostname}: before_apic_oper 状態採取完了（apic）",
                    )
                    log_detail(
                        log_directory,
                        pid,
                        f"before_apic_oper={scenario_directory}/apic_{scenario}_before_oper_statuses.json",
                    )

                    if not apic_oper_status_list:
                        log_step(main_log_path, f"[STEP{step}/{steps}]APIC片寄 ERROR")
                        log_step(
                            main_log_path,
                            f"ノード切り離し:{node_type.capitalize()} ERROR",
                        )
                        log_processing(
                            log_directory,
                            pid,
                            f"{hostname}: APICポートOperStatusダウンあり",
                        )
                        raise RuntimeError("APICポートOperStatusダウンあり")
                    else:
                        log_processing(
                            log_directory, pid, f"{hostname}: shutdown投入開始"
                        )
                        log_detail(
                            log_directory,
                            pid,
                            f"{hostname}: POSTファイル={apic_shutdown_file}",
                        )

                        resp = post_file(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            apic_shutdown_file,
                            log_directory=log_directory,
                            uid=pid,
                        )

                        if resp is False:
                            log_step(
                                main_log_path, f"[STEP{step}/{steps}]APIC片寄 ERROR"
                            )
                            log_step(
                                main_log_path,
                                f"ノード切り離し:{node_type.capitalize()} ERROR",
                            )
                            update_node_status(
                                log_directory,
                                pid,
                                hostname,
                                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                f"{hostname}: POST失敗: {apic_shutdown_file}",
                            )
                            continue

                        log_processing(
                            log_directory, pid, f"{hostname}: shutdown投入完了"
                        )

                        log_processing(
                            log_directory, pid, f"{timestamp()} {credentials.AFTER_ENABLE_DISABLE_SLEEP}秒待機中..."
                        )
                        time.sleep(credentials.AFTER_ENABLE_DISABLE_SLEEP)

                        other_apic_leaf_admin_status_list = get_apic_admin_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            other_apic_leaf_dn_list,
                            f"{scenario_directory}/other_apic_leaf_{scenario}_before_admin_statuses.json",
                            check_target="up",
                        )
                        other_apic_leaf_oper_status_list = get_apic_oper_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            other_apic_leaf_dn_list,
                            f"{scenario_directory}/other_apic_leaf_{scenario}_before_oper_statuses.json",
                            check_target="up",
                        )
                        apic_leaf_admin_status_list = get_apic_admin_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            apic_leaf_dn_list,
                            f"{scenario_directory}/apic_leaf_{scenario}_before_admin_statuses.json",
                            check_target="down",
                        )
                        apic_leaf_oper_status_list = get_apic_oper_statuses(
                            token,
                            apic_ip,
                            node_id,
                            pod_id,
                            hostname,
                            apic_leaf_dn_list,
                            f"{scenario_directory}/apic_leaf_{scenario}_before_oper_statuses.json",
                            check_target="down",
                        )
                        log_processing(
                            log_directory,
                            pid,
                            f"{hostname}: before 状態採取完了（apic）",
                        )
                        log_detail(
                            log_directory,
                            pid,
                            f"{hostname}: before_other_apic_leaf_admin={scenario_directory}/other_apic_leaf_{scenario}_before_admin_statuses.json, "
                            f"before_other_apic_leaf_oper={scenario_directory}/other_apic_leaf_{scenario}_before_oper_statuses.json, "
                            f"before_apic_leaf_admin={scenario_directory}/apic_leaf_{scenario}_before_admin_statuses.json, "
                            f"before_apic_leaf_oper={scenario_directory}/apic_leaf_{scenario}_before_oper_statuses.json",
                        )

                        if (
                            not other_apic_leaf_admin_status_list
                            or not other_apic_leaf_oper_status_list
                            or not apic_leaf_admin_status_list
                            or not apic_leaf_oper_status_list
                        ):
                            # Correct: if not other_apic_leaf_admin_status_list or not other_apic_leaf_oper_status_list or not apic_leaf_admin_status_list or not apic_leaf_oper_status_list:
                            log_step(
                                main_log_path, f"[STEP{step}/{steps}]APIC片寄 ERROR"
                            )
                            log_step(
                                main_log_path,
                                f"ノード切り離し:{node_type.capitalize()} ERROR",
                            )
                            log_processing(
                                log_directory,
                                pid,
                                f"{hostname}: APICポートStatus想定外あり",
                            )
                            raise RuntimeError("APICポートStatus想定外あり")
                        else:

                            log_step(main_log_path, f"[STEP{step}/{steps}]APIC片寄 END")
                            step += 1

                            (
                                spine_shutdown_file,
                                dn_list,
                                spine_dn_list,
                                spine_apic_leaf_dn_list,
                                apic_leaf_hostname,
                            ) = create_spine_shutdown(
                                token, apic_ip, hostname, node_id, pod_id, uid
                            )
                            log_processing(
                                log_directory, pid, f"{hostname}: shutdown/noshut 生成"
                            )
                            log_detail(
                                log_directory,
                                pid,
                                f"{hostname}: shutdown_file={spine_shutdown_file}, dn_count={len(dn_list)}",
                            )

                            spine_leaf_admin_status_list = get_spine_admin_statuses(
                                token,
                                apic_ip,
                                node_id,
                                pod_id,
                                hostname,
                                spine_apic_leaf_dn_list,
                                f"{scenario_directory}/{hostname}_{scenario}_before_spine_leaf_admin_statuses.json",
                                check_target="up",
                            )
                            spine_leaf_oper_status_list = get_spine_oper_statuses(
                                token,
                                apic_ip,
                                node_id,
                                pod_id,
                                hostname,
                                spine_apic_leaf_dn_list,
                                f"{scenario_directory}/{hostname}_{scenario}_before_spine_leaf_oper_statuses.json",
                                check_target="up",
                            )
                            log_processing(
                                log_directory,
                                pid,
                                f"{hostname}: before leaf-spine 状態採取完了（spine）",
                            )
                            log_detail(
                                log_directory,
                                pid,
                                f"{hostname}: before_admin={scenario_directory}/{hostname}_{scenario}_before_spine_leaf_admin_statuses.json, "
                                f"before_oper={scenario_directory}/{hostname}_{scenario}_before_spine_leaf_oper_statuses.json",
                            )

                            if (
                                not spine_leaf_admin_status_list
                                or not spine_leaf_oper_status_list
                            ):
                                log_step(
                                    main_log_path,
                                    f"[STEP{step}/{steps}]ノード切り離し ERROR",
                                )
                                log_step(
                                    main_log_path,
                                    f"ノード切り離し:{node_type.capitalize()} ERROR",
                                )
                                log_processing(
                                    log_directory,
                                    pid,
                                    f"{hostname}: apic_leaf-spineポートStatus想定外あり",
                                )
                                raise RuntimeError(
                                    "apic_leaf-spineポートStatus想定外あり"
                                )
                            else:
                                admin_status_list = get_spine_admin_statuses(
                                    token,
                                    apic_ip,
                                    node_id,
                                    pod_id,
                                    hostname,
                                    spine_dn_list,
                                    f"{scenario_directory}/{hostname}_{scenario}_before_admin_statuses.json",
                                    check_target="up",
                                )
                                oper_status_list = get_spine_oper_statuses(
                                    token,
                                    apic_ip,
                                    node_id,
                                    pod_id,
                                    hostname,
                                    spine_dn_list,
                                    f"{scenario_directory}/{hostname}_{scenario}_before_oper_statuses.json",
                                    check_target="up",
                                )
                                log_processing(
                                    log_directory,
                                    pid,
                                    f"{hostname}: before 状態採取完了（spine）",
                                )
                                log_detail(
                                    log_directory,
                                    pid,
                                    f"{hostname}: before_admin={scenario_directory}/{hostname}_{scenario}_before_admin_statuses.json, "
                                    f"before_oper={scenario_directory}/{hostname}_{scenario}_before_oper_statuses.json",
                                )

                                log_step(
                                    main_log_path,
                                    f"[STEP{step}/{steps}]Spine再起動&ノード切り離し START",
                                )
                                log_processing(
                                    log_directory, pid, f"{hostname}: reload投入開始"
                                )
                                reloaded = reload_node(
                                    token,
                                    apic_ip,
                                    node_id,
                                    pod_id,
                                    log_directory=log_directory,
                                    uid=pid,
                                )

                                if not reloaded:
                                    log_processing(
                                        log_directory,
                                        pid,
                                        f"{hostname}: reload失敗 -> 全体停止",
                                    )
                                    log_detail(
                                        log_directory,
                                        pid,
                                        f"{hostname}: reload 失敗により処理中断",
                                    )
                                    log_step(
                                        main_log_path,
                                        f"[STEP{step}/{steps}]Spine再起動&ノード切り離し ERROR",
                                    )
                                    log_step(
                                        main_log_path,
                                        f"ノード切り離し:{node_type.capitalize()} ERROR",
                                    )
                                    update_node_status(
                                        log_directory,
                                        pid,
                                        hostname,
                                        result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                        f"{hostname}: {hostname} の reload 失敗",
                                    )
                                    continue
                                else:
                                    log_processing(
                                        log_directory,
                                        pid,
                                        f"{hostname}: reload投入完了",
                                    )

                                    time.sleep(3)

                                    excluded = {hostname}
                                    other_spines = {
                                        k: v
                                        for k, v in spines.items()
                                        if k not in excluded
                                    }
                                    commands_file = (
                                        f"{commands_directory}other_spine_commands.txt"
                                    )

                                    log_detail(
                                        log_directory,
                                        pid,
                                        f"{hostname}: wait_for_spine_status(desired=inactive, timeout=1800, interval=60)",
                                    )
                                    inactive = wait_for_spine_status(
                                        hostname,
                                        desired_status=False,
                                        timeout=1800,
                                        interval=60,
                                    )

                                    if not inactive:
                                        log_step(
                                            main_log_path,
                                            f"[STEP{step}/{steps}]Spine再起動&ノード切り離し ERROR",
                                        )
                                        log_step(
                                            main_log_path,
                                            f"ノード切り離し:{node_type.capitalize()} ERROR",
                                        )
                                        log_processing(
                                            log_directory,
                                            pid,
                                            f"{hostname}: inactive待ちタイムアウト",
                                        )
                                        log_detail(
                                            log_directory,
                                            pid,
                                            f"{hostname}: wait_for_spine_status False（desired=inactive）",
                                        )
                                        raise TimeoutError(
                                            "Spine が inactive にならない"
                                        )
                                    else:
                                        log_processing(
                                            log_directory,
                                            pid,
                                            f"{hostname}: inactive 確認OK",
                                        )

                                        try:
                                            log_processing(
                                                log_directory,
                                                pid,
                                                f"APIC接続試行: token_node={token_node}",
                                            )
                                            token, apic_ip, apic = (
                                                get_token_from_random_node(token_node)
                                            )
                                            log_processing(
                                                log_directory,
                                                pid,
                                                f"APIC接続成功: apic_ip={apic_ip}, apic={apic}",
                                            )
                                        except Exception as e:
                                            log_processing(
                                                log_directory, pid, "APIC接続失敗"
                                            )
                                            log_detail(
                                                log_directory,
                                                pid,
                                                f"APIC接続例外: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                                            )
                                            print(
                                                f"{timestamp()} Failed to retrieve APIC token or IP."
                                            )
                                            log_step(
                                                main_log_path,
                                                f"[STEP{step}/{steps}]Spine再起動&ノード切り離し ERROR",
                                            )
                                            log_step(
                                                main_log_path,
                                                f"ノード切り離し:{node_type.capitalize()} ERROR",
                                            )
                                            update_node_status(
                                                log_directory,
                                                pid,
                                                hostname,
                                                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                                f"{hostname}: APIC接続失敗",
                                            )
                                            continue

                                        exceptions = []

                                        def get_logs_wrapper(*args, **kwargs):
                                            try:
                                                get_logs(*args, **kwargs)
                                            except Exception as e:
                                                exceptions.append(e)

                                        threads = []

                                        for spine, ip in other_spines.items():
                                            log_processing(
                                                log_directory,
                                                pid,
                                                f"{spine}: {commands_file} SSHログ採取開始（spine）",
                                            )
                                            t = threading.Thread(
                                                target=get_logs_wrapper,
                                                args=(
                                                    spine,
                                                    ip,
                                                    apic_username,
                                                    apic_password,
                                                    commands_file,
                                                    f"{scenario_directory}/{log_timestamp()}_{spine}_other_spine.log",
                                                ),
                                            )
                                            t.start()
                                            threads.append(t)

                                        for t in threads:
                                            t.join()

                                        if exceptions:
                                            for e in exceptions:
                                                print(f"例外発生: {e}")
                                                log_processing(
                                                    log_directory, pid, f"例外発生: {e}"
                                                )

                                            log_processing(
                                                log_directory, pid, "SSHログ採取失敗"
                                            )
                                            log_detail(
                                                log_directory,
                                                pid,
                                                f"SSHログ採取失敗: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                                            )
                                            print(
                                                f"{timestamp()} Failed to get SSH logs."
                                            )
                                            log_step(
                                                main_log_path,
                                                f"[STEP{step}/{steps}]Spine再起動&ノード切り離し ERROR",
                                            )
                                            log_step(
                                                main_log_path,
                                                f"ノード切り離し:{node_type.capitalize()} ERROR",
                                            )
                                            update_node_status(
                                                log_directory,
                                                pid,
                                                hostname,
                                                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                                f"{hostname}: SSHログ採取失敗",
                                            )
                                            continue

                                        log_processing(
                                            log_directory,
                                            pid,
                                            f"{spine}: {commands_file} SSHログ採取完了（spine）",
                                        )

                                        log_processing(
                                            log_directory,
                                            pid,
                                            f"{hostname}: shutdown投入開始",
                                        )
                                        log_detail(
                                            log_directory,
                                            pid,
                                            f"{hostname}: POSTファイル={spine_shutdown_file}, dn_count={len(dn_list)}",
                                        )
                                        resp = post_file(
                                            token,
                                            apic_ip,
                                            node_id,
                                            pod_id,
                                            spine_shutdown_file,
                                            log_directory=log_directory,
                                            uid=pid,
                                        )

                                        if resp is False:
                                            log_step(
                                                main_log_path,
                                                f"[STEP{step}/{steps}]Spine再起動&ノード切り離し ERROR",
                                            )
                                            log_step(
                                                main_log_path,
                                                f"ノード切り離し:{node_type.capitalize()} ERROR",
                                            )
                                            update_node_status(
                                                log_directory,
                                                pid,
                                                hostname,
                                                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                                f"{hostname}: POST失敗: {spine_shutdown_file}",
                                            )
                                            continue

                                        log_processing(
                                            log_directory,
                                            pid,
                                            f"{hostname}: shutdown投入完了",
                                        )

                                        time.sleep(10)

                                        log_detail(
                                            log_directory,
                                            pid,
                                            f"{hostname}: wait_for_spine_status(desired=active, timeout=1800, interval=60)",
                                        )
                                        active = wait_for_spine_status(
                                            hostname,
                                            desired_status=True,
                                            timeout=1800,
                                            interval=60,
                                        )

                                        if not active:
                                            log_step(
                                                main_log_path,
                                                f"[STEP{step}/{steps}]Spine再起動&ノード切り離し ERROR",
                                            )
                                            log_step(
                                                main_log_path,
                                                f"ノード切り離し:{node_type.capitalize()} ERROR",
                                            )
                                            log_processing(
                                                log_directory,
                                                pid,
                                                f"{hostname}: active待ちタイムアウト",
                                            )
                                            log_detail(
                                                log_directory,
                                                pid,
                                                f"{hostname}: wait_for_spine_status True（desired=active）",
                                            )
                                            raise TimeoutError(
                                                "Spine が active にならない"
                                            )
                                        else:
                                            log_step(
                                                main_log_path,
                                                f"[STEP{step}/{steps}]Spine再起動&ノード切り離し END",
                                            )
                                            log_step(
                                                main_log_path,
                                                f"[STEP{step}/{steps}]事後確認 START",
                                            )
                                            log_processing(
                                                log_directory,
                                                pid,
                                                f"{hostname}: active 確認OK",
                                            )

                                            try:
                                                log_processing(
                                                    log_directory,
                                                    pid,
                                                    f"APIC接続試行: token_node={token_node}",
                                                )
                                                token, apic_ip, apic = (
                                                    get_token_from_random_node(
                                                        token_node
                                                    )
                                                )
                                                log_processing(
                                                    log_directory,
                                                    pid,
                                                    f"APIC接続成功: apic_ip={apic_ip}, apic={apic}",
                                                )
                                            except Exception as e:
                                                log_processing(
                                                    log_directory, pid, "APIC接続失敗"
                                                )
                                                log_detail(
                                                    log_directory,
                                                    pid,
                                                    f"APIC接続例外: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                                                )
                                                print(
                                                    f"{timestamp()} Failed to retrieve APIC token or IP."
                                                )
                                                log_step(
                                                    main_log_path,
                                                    f"[STEP{step}/{steps}]事後確認 ERROR",
                                                )
                                                log_step(
                                                    main_log_path,
                                                    f"ノード切り離し:{node_type.capitalize()} ERROR",
                                                )
                                                update_node_status(
                                                    log_directory,
                                                    pid,
                                                    hostname,
                                                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                                    f"{hostname}: APIC接続失敗",
                                                )
                                                continue

                                            included = {hostname}
                                            target_spine = {
                                                k: v
                                                for k, v in spines.items()
                                                if k in included
                                            }
                                            commands_file = f"{commands_directory}spine_commands.txt"

                                            for spine, ip in target_spine.items():
                                                log_processing(
                                                    log_directory,
                                                    pid,
                                                    f"{spine}: {commands_file} SSHログ採取開始（spine）",
                                                )
                                                get_logs(
                                                    spine,
                                                    ip,
                                                    apic_username,
                                                    apic_password,
                                                    commands_file,
                                                    f"{scenario_directory}/{log_timestamp()}_{spine}_spine.log",
                                                )
                                                log_processing(
                                                    log_directory,
                                                    pid,
                                                    f"{spine}: {commands_file} SSHログ採取完了（spine）",
                                                )

                                            time.sleep(300)

                                            commands_file = f"{commands_directory}all_spine_commands.txt"

                                            exceptions = []

                                            def get_logs_wrapper(*args, **kwargs):
                                                try:
                                                    get_logs(*args, **kwargs)
                                                except Exception as e:
                                                    exceptions.append(e)

                                            threads = []

                                            for spine, ip in spines.items():
                                                log_processing(
                                                    log_directory,
                                                    pid,
                                                    f"{spine}: {commands_file} SSHログ採取開始（spine）",
                                                )
                                                t = threading.Thread(
                                                    target=get_logs_wrapper,
                                                    args=(
                                                        spine,
                                                        ip,
                                                        apic_username,
                                                        apic_password,
                                                        commands_file,
                                                        f"{scenario_directory}/{log_timestamp()}_{spine}_{scenario}_after_all_spine.log",
                                                    ),
                                                )
                                                t.start()
                                                threads.append(t)

                                            for t in threads:
                                                t.join()

                                            if exceptions:
                                                for e in exceptions:
                                                    print(f"例外発生: {e}")
                                                    log_processing(
                                                        log_directory,
                                                        pid,
                                                        f"例外発生: {e}",
                                                    )

                                                log_processing(
                                                    log_directory,
                                                    pid,
                                                    "SSHログ採取失敗",
                                                )
                                                log_detail(
                                                    log_directory,
                                                    pid,
                                                    f"SSHログ採取失敗: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                                                )
                                                print(
                                                    f"{timestamp()} Failed to get SSH logs."
                                                )
                                                log_step(
                                                    main_log_path,
                                                    f"[STEP{step}/{steps}]事後確認 ERROR",
                                                )
                                                log_step(
                                                    main_log_path,
                                                    f"ノード切り離し:{node_type.capitalize()} ERROR",
                                                )
                                                update_node_status(
                                                    log_directory,
                                                    pid,
                                                    hostname,
                                                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                                    f"{hostname}: SSHログ採取失敗",
                                                )
                                                continue

                                            check_commands_file = f"{commands_directory}check_commands.txt"

                                            with open(check_commands_file, "r") as f:
                                                lines = f.readlines()

                                            exceptions = []

                                            def get_check_logs_wrapper(*args, **kwargs):
                                                try:
                                                    get_check_logs(*args, **kwargs)
                                                except Exception as e:
                                                    exceptions.append(e)

                                            threads = []

                                            for line in lines:
                                                words = line.strip().split()
                                                threads = (
                                                    []
                                                )  # ループごとにリストを初期化
                                                for spine, ip in spines.items():
                                                    log_processing(
                                                        log_directory,
                                                        pid,
                                                        f"{spine}: {words[1]} SSHログ採取開始（spine）",
                                                    )
                                                    t = threading.Thread(
                                                        target=get_check_logs_wrapper,
                                                        args=(
                                                            spine,
                                                            ip,
                                                            apic_username,
                                                            apic_password,
                                                            line.strip(),
                                                            f"{scenario_directory}/{spine}_{scenario}_after_{words[1]}.log",
                                                        ),
                                                    )
                                                    t.start()
                                                    threads.append((t, spine))

                                                for t, spine in threads:
                                                    t.join()

                                            if exceptions:
                                                for e in exceptions:
                                                    print(f"例外発生: {e}")
                                                    log_processing(
                                                        log_directory,
                                                        pid,
                                                        f"例外発生: {e}",
                                                    )

                                                log_processing(
                                                    log_directory,
                                                    pid,
                                                    "SSHログ採取失敗",
                                                )
                                                log_detail(
                                                    log_directory,
                                                    pid,
                                                    f"SSHログ採取失敗: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                                                )
                                                print(
                                                    f"{timestamp()} Failed to get SSH logs."
                                                )
                                                log_step(
                                                    main_log_path,
                                                    f"[STEP{step}/{steps}]事後確認 ERROR",
                                                )
                                                log_step(
                                                    main_log_path,
                                                    f"ノード切り離し:{node_type.capitalize()} ERROR",
                                                )
                                                update_node_status(
                                                    log_directory,
                                                    pid,
                                                    hostname,
                                                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                                    f"{hostname}: SSHログ採取失敗",
                                                )
                                                continue

                                            check_commands_word_list = []

                                            print(lines)

                                            for l in lines:
                                                w = l.strip().split()
                                                check_commands_word_list.append(w[1])

                                            print(check_commands_word_list)

                                            diff_list = []

                                            for word in check_commands_word_list:
                                                # === Compare SSH logs per spine (disable): before vs after ===
                                                for spine in spines.keys():
                                                    before_path = f"{scenario_directory}/{spine}_{scenario}_before_{word}.log"
                                                    after_path = f"{scenario_directory}/{spine}_{scenario}_after_{word}.log"
                                                    diff_out = f"{scenario_directory}/{spine}_{scenario}_{word}_diff_unified.txt"

                                                    if (
                                                        spine == hostname
                                                        and word == "lldp"
                                                    ):
                                                        only_apic_leaf_naighbor = check_only_neighbor_from_file(
                                                            after_path,
                                                            apic_leaf_hostname,
                                                        )
                                                        if only_apic_leaf_naighbor:
                                                            log_processing(
                                                                log_directory,
                                                                pid,
                                                                f"{spine}: lldp neighbors確認（{word} only apic leaf）= OK",
                                                            )
                                                        else:
                                                            log_processing(
                                                                log_directory,
                                                                pid,
                                                                f"{spine}: lldp neighbors確認（disable {word} before vs after）= 差分あり",
                                                            )
                                                            log_detail(
                                                                log_directory,
                                                                pid,
                                                                f"{spine}: lldp neighbors確認 -> 想定外あり",
                                                            )
                                                            diff_list.append(word)
                                                    elif (
                                                        spine == hostname
                                                        and word == "interface"
                                                    ):
                                                        pass
                                                    elif (
                                                        spine == hostname
                                                        and word == "isis"
                                                    ):
                                                        pass
                                                    elif word == "module":
                                                        pass
                                                    elif word == "diagnostic":
                                                        pass
                                                    else:
                                                        identical = compare_ssh_logs(
                                                            file_before=before_path,
                                                            file_after=after_path,
                                                            diff_out=diff_out,
                                                            log_directory=log_directory,
                                                            uid=pid,
                                                            hostname=spine,
                                                        )
                                                        # optional: react to diffs
                                                        if identical:
                                                            log_processing(
                                                                log_directory,
                                                                pid,
                                                                f"{spine}: SSHログ比較（disable {word} before vs after）= OK",
                                                            )
                                                        else:
                                                            log_processing(
                                                                log_directory,
                                                                pid,
                                                                f"{spine}: SSHログ比較（disable {word} before vs after）= 差分あり",
                                                            )
                                                            log_detail(
                                                                log_directory,
                                                                pid,
                                                                f"{spine}: 差分ファイル -> {diff_out}",
                                                            )
                                                            diff_list.append(word)

                                            # 1) AFTER 側の状態採取（spine/self, apic-leaf 間）

                                            admin_status_list = get_spine_admin_statuses(
                                                token,
                                                apic_ip,
                                                node_id,
                                                pod_id,
                                                hostname,
                                                spine_dn_list,
                                                f"{scenario_directory}/{hostname}_{scenario}_after_admin_statuses.json",
                                                check_target="down",
                                            )
                                            oper_status_list = get_spine_oper_statuses(
                                                token,
                                                apic_ip,
                                                node_id,
                                                pod_id,
                                                hostname,
                                                spine_dn_list,
                                                f"{scenario_directory}/{hostname}_{scenario}_after_oper_statuses.json",
                                                check_target="down",
                                            )

                                            spine_leaf_admin_status_list = get_spine_admin_statuses(
                                                token,
                                                apic_ip,
                                                node_id,
                                                pod_id,
                                                hostname,
                                                spine_apic_leaf_dn_list,
                                                f"{scenario_directory}/{hostname}_{scenario}_after_spine_leaf_admin_statuses.json",
                                                check_target="up",
                                            )
                                            spine_leaf_oper_status_list = get_spine_oper_statuses(
                                                token,
                                                apic_ip,
                                                node_id,
                                                pod_id,
                                                hostname,
                                                spine_apic_leaf_dn_list,
                                                f"{scenario_directory}/{hostname}_{scenario}_after_spine_leaf_oper_statuses.json",
                                                check_target="up",
                                            )

                                            log_processing(
                                                log_directory,
                                                pid,
                                                f"{hostname}: AFTER(spine admin down={bool(admin_status_list)}, oper down={bool(oper_status_list)})",
                                            )
                                            log_processing(
                                                log_directory,
                                                pid,
                                                f"{hostname}: AFTER(apic leaf admin up={bool(spine_leaf_admin_status_list)}, "
                                                f"oper up={bool(spine_leaf_oper_status_list)})",
                                            )

                                            # 2) SSHログ比較（diff_list はどこか前の処理で作ってる前提）
                                            if diff_list:
                                                log_processing(
                                                    log_directory,
                                                    pid,
                                                    f"{spine}: SSHログ比較（disable before vs after）= 差分あり",
                                                )
                                                log_detail(
                                                    log_directory,
                                                    pid,
                                                    f"{spine}: 差分 -> {diff_list}",
                                                )

                                            diff_ok = not bool(diff_list)

                                            # 3) Module チェック
                                            log_processing(
                                                log_directory,
                                                pid,
                                                f"{hostname}: Module 確認開始（spine）",
                                            )
                                            modules_ok, modules_msg = (
                                                analyze_show_module_log(
                                                    f"{scenario_directory}/{hostname}_{scenario}_after_module.log"
                                                )
                                            )

                                            # 4) Diag チェック
                                            log_processing(
                                                log_directory,
                                                pid,
                                                f"{hostname}: Diagnostic 確認開始（spine）",
                                            )
                                            diag_ok, diag_msg = analyze_diag_result_log(
                                                f"{scenario_directory}/{hostname}_{scenario}_after_diagnostic.log"
                                            )

                                            # 5) それぞれを bool と NG理由にまとめる

                                            admin_ok = bool(admin_status_list)
                                            oper_ok = bool(oper_status_list)
                                            spine_leaf_admin_ok = bool(
                                                spine_leaf_admin_status_list
                                            )
                                            spine_leaf_oper_ok = bool(
                                                spine_leaf_oper_status_list
                                            )

                                            reasons = []

                                            if not admin_ok:
                                                reasons.append(
                                                    "spine adminSt が想定どおり down になっていない"
                                                )
                                            if not oper_ok:
                                                reasons.append(
                                                    "spine operSt が想定どおり down になっていない"
                                                )
                                            if not spine_leaf_admin_ok:
                                                reasons.append(
                                                    "spine–apic_leaf 間 adminSt が想定どおり up になっていない"
                                                )
                                            if not spine_leaf_oper_ok:
                                                reasons.append(
                                                    "spine–apic_leaf 間 operSt が想定どおり up になっていない"
                                                )
                                            if not diff_ok:
                                                reasons.append("SSHログ比較で差分あり")
                                            if not modules_ok:
                                                reasons.append(
                                                    f"Module 確認NG: {modules_msg}"
                                                )
                                            if not diag_ok:
                                                reasons.append(
                                                    f"DIAG TEST NG: {diag_msg}"
                                                )

                                            all_ok = all(
                                                [
                                                    admin_ok,
                                                    oper_ok,
                                                    spine_leaf_admin_ok,
                                                    spine_leaf_oper_ok,
                                                    diff_ok,
                                                    modules_ok,
                                                    diag_ok,
                                                ]
                                            )
                                            

                                            # ★追加: BL-SW トラフィック確認（all_ok=True の時のみ）
                                            blsw_ok = True
                                            blsw_ng_msg = ""
                                            if all_ok and blsw_traffic_check.is_enabled():
                                                area = get_area_network(hostname)
                                                if not area:
                                                    blsw_ok = False
                                                    blsw_ng_msg = "BL-SWトラフィック確認不可(area_network取得不可)"
                                                    log_processing(
                                                        log_directory, pid,
                                                        f"BL-SWトラフィック確認: {hostname} area取得不可",
                                                    )
                                                else:
                                                    log_processing(
                                                        log_directory, pid,
                                                        f"BL-SWトラフィック確認開始: area={area}",
                                                    )
                                                    try:
                                                        blsw_ok = blsw_traffic_check.check_area(
                                                            area,
                                                            log=lambda m: log_detail(
                                                                log_directory, pid, f"[BLSW] {m}"
                                                            ),
                                                        )
                                                        log_processing(
                                                            log_directory, pid,
                                                            f"BL-SWトラフィック確認 {'OK' if blsw_ok else 'NG'}: area={area}",
                                                        )
                                                        if not blsw_ok:
                                                            blsw_ng_msg = "BL-SWトラフィック確認NG"
                                                    except blsw_traffic_check.GrafanaCheckError as e:
                                                        blsw_ok = False
                                                        blsw_ng_msg = "BL-SWトラフィック確認不可(判定不能)"
                                                        log_processing(
                                                            log_directory, pid,
                                                            f"BL-SWトラフィック確認 判定不能: area={area}",
                                                        )
                                                        log_detail(
                                                            log_directory, pid,
                                                            f"[BLSW] {area}: {type(e).__name__}: {e}",
                                                        )

                                            # 6) 最後の if で一括判定

                                            if all_ok and blsw_ok:
                                                log_step(
                                                    main_log_path,
                                                    f"[STEP{step}/{steps}]事後確認 END",
                                                )
                                                step += 1
                                                update_node_status(
                                                    log_directory,
                                                    pid,
                                                    hostname,
                                                    result_code.EACH_STATUS_CODE_COMPLETED,
                                                    f"{hostname}の切り離し正常終了",
                                                )
                                            elif all_ok and not blsw_ok:    # ★ 追加（この分岐を新設）
                                                log_step(
                                                    main_log_path,
                                                    f"[STEP{step}/{steps}]事後確認 ERROR",
                                                )
                                                log_step(
                                                    main_log_path,
                                                    f"ノード切り離し:{node_type.capitalize()} ERROR",
                                                )
                                                log_processing(
                                                    log_directory, pid,
                                                    f"{hostname}: {blsw_ng_msg}",
                                                )
                                                update_node_status(
                                                    log_directory,
                                                    pid,
                                                    hostname,
                                                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                                    f"{hostname}の{blsw_ng_msg}",
                                                )
                                            else:
                                                msg_detail = (
                                                    " / ".join(reasons)
                                                    if reasons
                                                    else "不明な理由でNG"
                                                )
                                                log_step(
                                                    main_log_path,
                                                    f"[STEP{step}/{steps}]事後確認 ERROR",
                                                )
                                                log_step(
                                                    main_log_path,
                                                    f"ノード切り離し:{node_type.capitalize()} ERROR",
                                                )

                                                log_processing(
                                                    log_directory,
                                                    pid,
                                                    f"{hostname}: 事後確認NG: {msg_detail}",
                                                )
                                                log_detail(
                                                    log_directory,
                                                    pid,
                                                    f"{hostname}: AFTER チェック結果 "
                                                    f"(admin_ok={admin_ok}, oper_ok={oper_ok}, "
                                                    f"spine_leaf_admin_ok={spine_leaf_admin_ok}, spine_leaf_oper_ok={spine_leaf_oper_ok}, "
                                                    f"diff_ok={diff_ok}, modules_ok={modules_ok}, diag_ok={diag_ok})",
                                                )

                                                update_node_status(
                                                    log_directory,
                                                    pid,
                                                    hostname,
                                                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                                                    f"{hostname}の事後確認NG: {msg_detail}",
                                                )

                                                # 全体止めたいなら:
                                                # fail_all_and_exit(log_directory, pid, hostnames, f"{hostname} 事後確認NG: {msg_detail}")
                                                # ノード単位のエラーだけにしたいなら ↑ をコメントアウトして、ループ継続でもOK

            except Exception as e:
                log_step(main_log_path, f"[STEP{step}/{steps}]{action} ERROR")
                log_step(main_log_path, f"{action}:{node_type.capitalize()} ERROR")
                log_processing(log_directory, pid, f"{hostname}: 例外発生 -> 異常終了")
                log_detail(
                    log_directory,
                    pid,
                    f"{hostname}: 例外 {type(e).__name__}: {e}\n{traceback.format_exc()}",
                )
                print(f"{timestamp()} {e}")
                update_node_status(
                    log_directory,
                    pid,
                    hostname,
                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                    f"{hostname}の切り離し異常終了",
                )

        if node_type == "leaf":
            step = step + 1

            log_step(main_log_path, f"[STEP{step}/{steps}]事後確認 START")
            run_blsw_check(log_directory, pid, hostnames)
            log_step(main_log_path, f"[STEP{step}/{steps}]事後確認 END")
            log_step(main_log_path, f"{action}:{node_type.capitalize()} END")

        finalize_status(log_directory, pid)

    elif scenario == "enable":

        # ==== enable シナリオ: run ディレクトリに溜めた JSON を各ノードへ投入し、事後確認を実施 ====
        uid_directory = f"{script_directory}/run/{uid}"
        if not os.path.exists(uid_directory):
            log_step(main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR")
            fail_all_and_exit(log_directory, pid, hostnames, f"ID不明: {uid}")

        nodes = hostnames

        threads = []

        step2_title = "Spine組み込み" if node_type == "spine" else "ノード組み込み"
        log_step(main_log_path, f"[STEP{step}/{steps}]{step2_title} START")

        for node in nodes:
            t = threading.Thread(
                target=post_threading,
                args=(
                    node,
                    uid,
                    pid,
                    uid_directory,
                    log_directory,
                    scenario_directory,
                    scenario,
                    apic_ip,
                    apic,
                    token,
                    node_type,
                    f"{log_directory}/{node}_{scenario}_statuses.json",
                    main_log_path,
                    step,
                    steps,
                    step2_title,
                ),
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        # ---- check if any node failed during enable ----
        status_path = f"{log_directory}/{pid}_status.json"
        with open(status_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        any_failed = any(
            n.get("each_status_code", "").startswith("E")
            for n in data.get("results", [])
        )

        if any_failed:
            if node_type == "spine":
                log_step(main_log_path, f"[STEP{step}/{steps}]Spine組み込み ERROR")
            else:
                log_step(main_log_path, f"[STEP{step}/{steps}]ノード組み込み ERROR")
            log_step(main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR")
        else:
            log_processing(log_directory, pid, "全ノード POST 完了")
            log_step(main_log_path, f"[STEP{step}/{steps}]{step2_title} END")

        step += 1

        if node_type == "spine":
            log_step(main_log_path, f"[STEP{step}/{steps}]APIC片寄解除 START")
            log_step(main_log_path, f"[STEP{step}/{steps}]APIC片寄解除 END")
            step += 1

        log_step(main_log_path, f"[STEP{step}/{steps}]事後確認 START")
        run_blsw_check(log_directory, pid, hostnames)
        finalize_status(log_directory, pid)
        log_step(main_log_path, f"[STEP{step}/{steps}]事後確認 END")
        log_step(main_log_path, f"ノード組み込み:{node_type.capitalize()} END")
        log_processing(log_directory, pid, "ノード組み込み 完了")


def post_threading(
    node,
    uid,
    pid,
    uid_directory,
    log_directory,
    scenario_directory,
    scenario,
    apic_ip,
    apic,
    token,
    node_type,
    out_path,
    main_log_path,
    step,
    steps,
    step2_title,
):
    # 個別ノードの enable 投入と採取・比較を行うスレッドワーカー
    log_processing(log_directory, pid, f"{node}: post_threading 開始")
    log_detail(
        log_directory,
        pid,
        f"{node}: node_directory={script_directory}/run/{uid}/{node}",
    )

    all_nodes = os.listdir(uid_directory)
    if node not in all_nodes:
        print(f"No hostname {node}")

        log_processing(
            log_directory, pid, f"{node}: {node} が見つかりません -> 異常終了"
        )
        update_node_status(
            f"{script_directory}/log/{uid}",
            pid,
            node,
            result_code.EACH_STATUS_CODE_SERVER_ERROR,
            f"{node}の組み込み異常終了（{node}が見つかりません）",
        )
        return

    node_directory = f"{script_directory}/run/{uid}/{node}"
    files = os.listdir(node_directory)

    # ---- collect noshut parts for this node ----
    pattern = re.compile(r".*part.*noshut\.json$")
    sorted_files = sorted(f for f in files if pattern.match(f))
    noshut_file = f"{node_directory}/{node}_noshut.json"

    # Build dn_list from node-level noshut file (used for after-status collection)
    dn_list = []
    if os.path.exists(noshut_file):
        with open(noshut_file, "r", encoding="utf-8") as f:
            for line in f:
                if "paths-" in line:
                    dn = line.split(r"\"")[-1]
                    dn_list.append(dn)
    else:
        log_processing(log_directory, pid, f"{node}: 配下不在 -> Spine/APIC向けのみ比較")

        # log_step(main_log_path, f"[STEP{step}/{steps}]{step2_title} ERROR")
        # log_step(main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR")
        # log_processing(log_directory, pid, f"{node}: {noshut_file} が見つかりません -> 異常終了")
        # update_node_status(f"{script_directory}/log/{uid}", pid, node, result_code.EACH_STATUS_CODE_SERVER_ERROR, f"{node}の組み込み異常終了（{node}_noshut.json 不在）")
        # return

    # ensure we actually had parts to post
    if not sorted_files and os.path.exists(noshut_file):
        log_step(main_log_path, f"[STEP{step}/{steps}]{step2_title} ERROR")
        log_step(main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR")
        log_processing(
            log_directory, pid, f"{node}: noshut パートファイルなし -> 異常終了"
        )
        update_node_status(
            f"{script_directory}/log/{uid}",
            pid,
            node,
            result_code.EACH_STATUS_CODE_SERVER_ERROR,
            f"{node}の組み込み異常終了（noshut分割ファイルなし）",
        )
        return

    # ---- POST each part (leaf or spine) ----

    try:
        node_id, pod_id = get_hostname_info(node, apic_ip, apic, token)
        log_detail(log_directory, pid, f"{node}: node_id={node_id}, pod_id={pod_id}")
    except Exception as e:
        log_step(main_log_path, f"[STEP{step}/{steps}]{step2_title} ERROR")
        log_step(main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR")
        log_processing(
            log_directory, pid, f"{node}: get_hostname_info 失敗 -> 異常終了"
        )
        log_detail(
            log_directory,
            pid,
            f"{node}: 例外 {type(e).__name__}: {e}\n{traceback.format_exc()}",
        )
        update_node_status(
            f"{script_directory}/log/{uid}",
            pid,
            node,
            result_code.EACH_STATUS_CODE_SERVER_ERROR,
            f"{node}の組み込み異常終了（node情報取得失敗）",
        )
        return

    for file in sorted_files:
        target_file = os.path.join(node_directory, file)
        resp = post_file(
            token,
            apic_ip,
            node_id,
            pod_id,
            target_file,
            log_directory=log_directory,
            uid=pid,
        )

        if resp is False:
            log_step(main_log_path, f"[STEP{step}/{steps}]{step2_title} ERROR")
            log_step(main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR")
            log_processing(
                log_directory,
                pid,
                f"{node}: {file} の POST 失敗 -> 異常終了（このノード）",
            )
            log_detail(log_directory, pid, f"{node}: 失敗ファイル={target_file}")
            update_node_status(
                f"{script_directory}/log/{uid}",
                pid,
                node,
                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                f"{node}の組み込み異常終了（POST失敗: {file}）",
            )
            return  # stop this node’s thread, other nodes keep running
        else:
            log_processing(log_directory, pid, f"{node}: {file} を POST 成功")
            log_detail(log_directory, pid, f"{node}: POST OK -> {target_file}")

        time.sleep(credentials.POST_FILE_SLEEP_INTERVAL)

    # ---- AFTER posting node noshut parts, post APIC noshut (spine only) ----
    if node_type == "spine":
        apic_noshut_path = os.path.join(node_directory, "apic_noshut.json")
        if any("apic" in f for f in files):
            if os.path.exists(apic_noshut_path):
                log_processing(
                    log_directory,
                    pid,
                    f"{node}: APIC noshut 事後投入開始 -> {apic_noshut_path}",
                )
                try:
                    # reuse node_id/pod_id context (OK per your environment)

                    resp = post_file(
                        token,
                        apic_ip,
                        node_id,
                        pod_id,
                        apic_noshut_path,
                        log_directory=log_directory,
                        uid=pid,
                    )

                    if resp is False:
                        log_step(
                            main_log_path, f"[STEP{step}/{steps}]APIC片寄解除 ERROR"
                        )  # spine の enable ではこのラベルでもOK
                        log_step(
                            main_log_path,
                            f"ノード組み込み:{node_type.capitalize()} ERROR",
                        )
                        log_processing(
                            log_directory,
                            pid,
                            f"{node}: APIC noshut POST失敗 -> 異常終了（このノード）",
                        )
                        log_detail(
                            log_directory,
                            pid,
                            f"{node}: 失敗ファイル={apic_noshut_path}",
                        )
                        update_node_status(
                            f"{script_directory}/log/{uid}",
                            pid,
                            node,
                            result_code.EACH_STATUS_CODE_SERVER_ERROR,
                            f"{node}の組み込み異常終了（APIC noshut POST失敗）",
                        )
                        return
                    else:
                        log_processing(
                            log_directory, pid, f"{node}: APIC noshut 事後投入完了"
                        )

                except Exception as e:
                    log_step(
                        main_log_path, f"[STEP{step}/{steps}]APIC片寄解除 ERROR"
                    )  # spine の enable ではこのラベルでもOK
                    log_step(
                        main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR"
                    )
                    log_processing(
                        log_directory,
                        pid,
                        f"{node}: APIC noshut 事後投入失敗 -> 異常終了",
                    )
                    log_detail(
                        log_directory,
                        pid,
                        f"{node}: APIC noshut POST 例外 {type(e).__name__}: {e}\n{traceback.format_exc()}",
                    )
                    update_node_status(
                        f"{script_directory}/log/{uid}",
                        pid,
                        node,
                        result_code.EACH_STATUS_CODE_SERVER_ERROR,
                        f"{node}の組み込み異常終了（APIC noshut 投入失敗）",
                    )
                    return
            else:
                log_step(
                    main_log_path, f"[STEP{step}/{steps}]APIC片寄解除 ERROR"
                )  # spine の enable ではこのラベルでもOK
                log_step(
                    main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR"
                )
                log_processing(
                    log_directory,
                    pid,
                    f"{node}: APIC noshut ファイル未検出 -> 異常終了",
                )
                log_detail(
                    log_directory, pid, f"{node}: 期待ファイルなし: {apic_noshut_path}"
                )
                update_node_status(
                    f"{script_directory}/log/{uid}",
                    pid,
                    node,
                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                    f"{node}の組み込み異常終了（APIC noshut ファイルなし）",
                )
                return

    # ---- compare DISABLE(before) vs ENABLE(after) for node ----

    log_processing(
                log_directory, pid, f"{timestamp()} {credentials.AFTER_ENABLE_DISABLE_SLEEP}秒待機中..."
            )
    time.sleep(credentials.AFTER_ENABLE_DISABLE_SLEEP)

    try:
        # Baseline must exist
        baseline_dir = f"{script_directory}/log/{uid}/disable"
        before_admin = f"{baseline_dir}/{node}_disable_before_admin_statuses.json"
        before_oper = f"{baseline_dir}/{node}_disable_before_oper_statuses.json"

        missing = []
        if not os.path.exists(before_admin):
            missing.append("before_admin")
        if not os.path.exists(before_oper):
            missing.append("before_oper")
        if missing and os.path.exists(noshut_file):
            log_step(main_log_path, f"[STEP{step}/{steps}]{step2_title} ERROR")
            log_step(main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR")
            log_processing(
                log_directory, pid, f"{node}: 基準ファイル欠如 {missing} -> 異常終了"
            )
            log_detail(log_directory, pid, f"{node}: baseline_dir={baseline_dir}")
            update_node_status(
                f"{script_directory}/log/{uid}",
                pid,
                node,
                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                f"{node}の組み込み異常終了（基準ファイル欠如: {', '.join(missing)}）",
            )
            return

        # Capture after files for node
        after_admin = f"{scenario_directory}/{node}_enable_after_admin_statuses.json"
        after_oper = f"{scenario_directory}/{node}_enable_after_oper_statuses.json"
        log_detail(
            log_directory,
            pid,
            f"{node}: after_admin={after_admin}, after_oper={after_oper}",
        )

        #time.sleep(60)

        if not os.path.exists(noshut_file):
            log_processing(
                log_directory, pid, f"{node}: 配下不在 -> Spine/APIC向けのみ比較"
            )
            cmp_admin = True
            cmp_oper = True
        else:

            if node_type == "leaf":
                _ = get_leaf_admin_statuses(
                    token,
                    apic_ip,
                    node_id,
                    pod_id,
                    node,
                    dn_list,
                    after_admin,
                    check_target="up",
                )
                _ = get_leaf_oper_statuses(
                    token,
                    apic_ip,
                    node_id,
                    pod_id,
                    node,
                    dn_list,
                    after_oper,
                    check_target="up",
                )

                
            else:
                _ = get_spine_admin_statuses(
                    token,
                    apic_ip,
                    node_id,
                    pod_id,
                    node,
                    dn_list,
                    after_admin,
                    check_target="up",
                )
                _ = get_spine_oper_statuses(
                    token,
                    apic_ip,
                    node_id,
                    pod_id,
                    node,
                    dn_list,
                    after_oper,
                    check_target="up",
                )

            # Compare node admin/oper
            cmp_admin = compare_status_reports(before_admin, after_admin)
            cmp_oper = compare_status_reports(before_oper, after_oper)
            log_processing(
                log_directory,
                pid,
                f"{node}: NODE admin 比較={ 'OK' if cmp_admin else 'NG' }, oper 比較={ 'OK' if cmp_oper else 'NG' }",
            )

        if node_type == "leaf":

            _, spine_dn_list = get_leaf_ports(
                token,
                apic_ip,
                node_id,
                pod_id
            )
            after_spine_admin = f"{scenario_directory}/{node}_enable_after_spine_admin_statuses.json"
            after_spine_oper = f"{scenario_directory}/{node}_enable_after_spine_oper_statuses.json"
            _ = get_leaf_admin_statuses(
                token,
                apic_ip,
                node_id,
                pod_id,
                node,
                spine_dn_list,
                after_spine_admin,
                check_target="up",
            )
            _ = get_leaf_oper_statuses(
                token,
                apic_ip,
                node_id,
                pod_id,
                node,
                spine_dn_list,
                after_spine_oper,
                check_target="up",
            )
            before_spine_admin = f"{script_directory}/log/{uid}/disable/{node}_disable_before_spine_admin_statuses.json"
            before_spine_oper = f"{script_directory}/log/{uid}/disable/{node}_disable_before_spine_oper_statuses.json"
            cmp_spine_admin = compare_status_reports(before_spine_admin, after_spine_admin) if os.path.exists(before_spine_admin) else False
            cmp_spine_oper = compare_status_reports(before_spine_oper, after_spine_oper) if os.path.exists(before_spine_oper) else False
            log_processing(
                log_directory,
                pid,
                f"{node}: SPINE admin 比較={ 'OK' if cmp_spine_admin else 'NG' }, oper 比較={ 'OK' if cmp_spine_oper else 'NG' }",
                )

            apic_dn_list = get_apic_ports(token, apic_ip, node_id, pod_id, up_only=False)
            apic_connected = any(f"node-{node_id}/" in dn for dn in apic_dn_list)

            if apic_connected:
                after_apic_admin = f"{scenario_directory}/{node}_enable_after_apic_admin_statuses.json"
                after_apic_oper = f"{scenario_directory}/{node}_enable_after_apic_oper_statuses.json"
                _ = get_apic_admin_statuses(
                    token, apic_ip, node_id, pod_id, node, apic_dn_list, after_apic_admin, check_target="up",
                )
                _ = get_apic_oper_statuses(
                    token, apic_ip, node_id, pod_id, node, apic_dn_list, after_apic_oper, check_target="up",
                )
                before_apic_admin = f"{script_directory}/log/{uid}/disable/{node}_disable_before_apic_admin_statuses.json"
                before_apic_oper = f"{script_directory}/log/{uid}/disable/{node}_disable_before_apic_oper_statuses.json"
                cmp_apic_admin = compare_status_reports(before_apic_admin, after_apic_admin) if os.path.exists(before_apic_admin) else False
                cmp_apic_oper = compare_status_reports(before_apic_oper, after_apic_oper) if os.path.exists(before_apic_oper) else False
                log_processing(
                    log_directory, pid,
                    f"{node}: APIC admin 比較={ 'OK' if cmp_apic_admin else 'NG' }, oper 比較={ 'OK' if cmp_apic_oper else 'NG' }",
                )
            else:
                log_processing(log_directory, pid, f"{node}: APICと非接続のためAPIC向けポート比較スキップ")
                cmp_apic_admin = True
                cmp_apic_oper = True

        # ---- APIC oper: compare disable-before vs enable-after (spine only) ----
        
        #detail = []
        if node_type == "spine":
            cmp_apic_oper = True  # default true for leaf
            try:
                # Build after APIC oper snapshot
                apic_dn_list = get_apic_ports(token, apic_ip, node_id, pod_id, up_only=False)
                apic_after_oper = (
                    f"{scenario_directory}/apic_enable_after_oper_statuses.json"
                )
                _ = get_apic_oper_statuses(
                    token,
                    apic_ip,
                    node_id,
                    pod_id,
                    node,
                    apic_dn_list,
                    apic_after_oper,
                    check_target="up",
                )

                apic_baseline_before_oper = f"{script_directory}/log/{uid}/disable/apic_disable_before_oper_statuses.json"
                if not os.path.exists(apic_baseline_before_oper):
                    cmp_apic_oper = False
                    log_processing(
                        log_directory,
                        pid,
                        f"{node}: APIC基準ファイル欠如 -> APIC比較NG",
                    )
                    log_detail(
                        log_directory,
                        pid,
                        f"{node}: missing {apic_baseline_before_oper}",
                    )
                else:
                    cmp_apic_oper = compare_status_reports(
                        apic_baseline_before_oper, apic_after_oper
                    )

                log_processing(
                    log_directory,
                    pid,
                    f"{node}: APIC oper 比較={ 'OK' if cmp_apic_oper else 'NG' }",
                )
            except Exception as e:
                cmp_apic_oper = False
                log_processing(
                    log_directory, pid, f"{node}: APIC oper 比較処理エラー -> NG"
                )
                log_detail(
                    log_directory,
                    pid,
                    f"{node}: APIC compare exception {type(e).__name__}: {e}\n{traceback.format_exc()}",
                )

        # Final decision
        # status_ok = bool(status_list) and cmp_admin and cmp_oper and (cmp_apic_oper if node_type == "spine" else True)
                
        #status_ok = (
        #    cmp_admin and cmp_oper and (cmp_apic_oper if node_type == "spine" else True)
        #)

        if node_type == "leaf":
            status_ok = cmp_admin and cmp_oper and cmp_spine_admin and cmp_spine_oper and cmp_apic_admin and cmp_apic_oper
        else:  # spine
            status_ok = cmp_admin and cmp_oper and cmp_apic_oper

        detail = []
        if not cmp_admin:
            detail.append("admin差分")
        if not cmp_oper:
            detail.append("oper差分")
        if node_type == "leaf":
            if not cmp_spine_admin:
                detail.append("Spine向けadmin差分")
            if not cmp_spine_oper:
                detail.append("Spine向けoper差分")
            if not cmp_apic_admin:
                detail.append("APIC向けadmin差分")
            if not cmp_apic_oper:
                detail.append("APIC向けoper差分")
        if node_type == "spine" and not cmp_apic_oper:
            detail.append("APIC oper差分")

        # if not status_list: detail.append("採取NG")
        detail_msg = " / ".join(detail) if detail else "不明"

        if status_ok:
            status_code = result_code.EACH_STATUS_CODE_COMPLETED
            message = f"{node}の組み込み正常終了（before-disable と after-enable 一致）"
            update_node_status(
                f"{script_directory}/log/{uid}", pid, node, status_code, message
            )
            log_processing(log_directory, pid, f"{node}: enable 正常終了")
        else:
            status_code = result_code.EACH_STATUS_CODE_SERVER_ERROR
            message = f"{node}の組み込み異常終了（{detail_msg}）"
            update_node_status(
                f"{script_directory}/log/{uid}", pid, node, status_code, message
            )
            log_processing(log_directory, pid, f"{node}: enable 異常終了")
            log_detail(log_directory, pid, f"{node}: 詳細 NG 要因 → {detail_msg}")

    except Exception as e:
        log_step(main_log_path, f"[STEP{step}/{steps}]{step2_title} ERROR")
        log_step(main_log_path, f"ノード組み込み:{node_type.capitalize()} ERROR")
        update_node_status(
            f"{script_directory}/log/{uid}",
            pid,
            node,
            result_code.EACH_STATUS_CODE_SERVER_ERROR,
            f"{node}の組み込み比較処理エラー: {type(e).__name__}: {e}",
        )
        log_processing(log_directory, pid, f"{node}: enable 比較処理例外 -> 異常終了")
        log_detail(
            log_directory,
            pid,
            f"{node}: 比較処理 例外 {type(e).__name__}: {e}\n{traceback.format_exc()}",
        )


### FOR TEST ENVIRONMENT ###
"""def hostname_exists(hostname):
        #DAI-3
    #hsts = ["tdqntys1-Leaf705", "tdqntys1-Leaf706", "tdqntys1-Leaf709", "tdqntys1-Leaf710", "tdqntys1-SpSw05", "tdqntys1-SpSw06"]

        #GIJIOYAMA
    hsts = ["tdqntys1-SpSw01", "tdqntys1-SpSw02", "tdqntys1-Leaf999", "tdqntys1-Leaf002", "tdqntys1-Leaf001", "tdqntys1-Leaf004", "tdqntys1-Leaf003", "tdqntys1-Leaf505", "tdqntys1-Leaf506", "tdqntys1-Leaf601", "tdqntys1-Leaf611", "tdqntys1-Leaf612", "tdqntys1-Leaf622", "tdqntys1-Leaf631", "tdqntys1-Leaf632", "tdqntys1-Leaf701", "tdqntys1-Leaf704", "tdqntys1-Leaf713", "tdqntys1-Leaf714", "tdqntys1-Leaf1981", "tdqntys1-Leaf1982", "tdqntys1-Leaf703", "tdqntys1-Leaf01", "tdqntys1-Leaf03", "tdqntys1-Leaf04", "tdqntys1-Leaf06", "tdqntys1-Leaf05", "tdqntys1-Leaf07", "tdqntys1-Leaf10", "tdqntys1-Leaf09", "tdqntys1-Leaf11", "tdqntys1-Leaf12", "tdqntys1-Leaf14", "tdqntys1-Leaf13", "tdqntys1-Leaf16", "tdqntys1-Leaf19", "tdqntys1-Leaf15", "tdqntys1-Leaf20", "tdqntys1-Leaf21", "tdqntys1-Leaf23", "tdqntys1-Leaf22", "tdqntys1-Leaf24", "tdqntys1-Leaf25", "tdqntys1-Leaf27", "tdqntys1-Leaf26", "tdqntys1-Leaf28", "tdqntys1-Leaf32", "tdqntys1-Leaf31", "tdqntys1-Leaf36", "tdqntys1-Leaf35", "tdqntys1-Leaf37", "tdqntys1-Leaf38", "tdqntys1-Leaf42", "tdqntys1-Leaf41", "tdqntys1-Leaf46", "tdqntys1-Leaf45", "tdqntys1-Leaf47", "tdqntys1-Leaf48", "tdqntys1-Leaf60", "tdqntys1-Leaf62", "tdqntys1-Leaf57", "tdqntys1-Leaf61", "tdqntys1-Leaf63", "tdqntys1-Leaf64", "tdqntys1-Leaf65", "DPI-leaf01", "tdqntys1-Leaf702", "tdqntys1-Leaf621", "tdqntys1-Leaf43", "tdqntys1-Leaf08", "tdqntys1-Leaf44", "tdqntys1-Leaf602"]

    if hostname in hsts:
        exist_hostname = True
        if exist_hostname:
            return hostname"""
### FOR TEST ENVIRONMENT ###


def hostname_exists(hostname):
    hostname_query = """
    SELECT 1
    FROM t_ch
    WHERE time = (SELECT MAX(time) FROM t_ch)
      AND hostname = %s
    LIMIT 1;
    """
    rows = fetch_from_psql(
        psql_host, psql_db, psql_user, psql_password, hostname_query, (hostname,)
    )
    return bool(rows)


def get_token_from_random_node(hostname):
    apic_ip, apic = apic_select(hostname)
    token = get_token(apic_ip, apic_username, apic_password)
    return token, apic_ip, apic


### FOR TEST ENVIRONMENT ###
"""def apic_select(hostname):
    apic_ips = ["172.30.98.65", "172.30.98.66", "172.30.98.67"]
    apic_ip = check_connection(apic_ips)
    apic = "tdqntys1-SdnSv0x"
    return apic_ip, apic"""
### FOR TEST ENVIRONMENT ###


def apic_select(hostname):
    area_network_query = """
    SELECT DISTINCT area_network FROM t_ch
    WHERE time = (SELECT MAX(time) FROM t_ch)
      AND hostname = %s;
    """
    area_network_rows = fetch_from_psql(
        psql_host, psql_db, psql_user, psql_password, area_network_query, (hostname,)
    )
    if not area_network_rows:
        raise RuntimeError(f"hostname '{hostname}' の area_network が見つかりません。")
    area_network = area_network_rows[0]

    apic_ips_query = """
    SELECT DISTINCT oobmgmt_ip FROM t_ch
    WHERE time = (SELECT MAX(time) FROM t_ch)
      AND area_network = %s
      AND role = 'controller';
    """
    apic_ips = fetch_from_psql(
        psql_host, psql_db, psql_user, psql_password, apic_ips_query, (area_network,)
    )
    apic_ip = check_connection(apic_ips)

    apic_query = """
    SELECT DISTINCT hostname FROM t_ch
    WHERE time = (SELECT MAX(time) FROM t_ch)
      AND area_network = %s
      AND role = 'controller'
      AND oobmgmt_ip = %s;
    """
    apic_rows = fetch_from_psql(
        psql_host,
        psql_db,
        psql_user,
        psql_password,
        apic_query,
        (area_network, apic_ip),
    )
    if not apic_rows:
        raise RuntimeError(
            f"area_network '{area_network}' に oobmgmt_ip={apic_ip} の controller が見つかりません。"
        )
    apic = apic_rows[0]
    return apic_ip, apic

def get_area_network(hostname):
    """hostname から area_network を取得する（取得不可なら None）。"""
    area_network_query = """
    SELECT DISTINCT area_network FROM t_ch
    WHERE time = (SELECT MAX(time) FROM t_ch)
      AND hostname = %s;
    """
    rows = fetch_from_psql(
        psql_host, psql_db, psql_user, psql_password, area_network_query, (hostname,)
    )
    return rows[0] if rows else None

def run_blsw_check(log_directory, uid, hostnames):
    if not blsw_traffic_check.is_enabled():
        log_processing(log_directory, uid, "BL-SWトラフィック確認: 無効(スキップ)")
        return

    # 既に E のノードを把握
    already_e = set()
    try:
        with open(f"{log_directory}/{uid}_status.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        for n in data.get("results", []):
            if str(n.get("each_status_code", "")).startswith("E"):
                already_e.add(n.get("target_node"))
    except Exception as e:
        log_detail(log_directory, uid, f"[BLSW] status.json 読込失敗（続行）: {e}")

    # 確認対象（E でないノード）
    targets = [h for h in hostnames if h not in already_e]
    if not targets:
        log_processing(log_directory, uid, "BL-SWトラフィック確認: 対象なし（全ノード既にNG）")
        return

    # area を1つ取る（対象の先頭から）
    area = get_area_network(targets[0])
    if not area:
        log_processing(log_directory, uid, "BL-SWトラフィック確認: area_network取得不可 -> 対象をE")
        for h in targets:
            update_node_status(log_directory, uid, h,
                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                f"{h}のBL-SWトラフィック確認不可(area_network取得不可)")
        return

    # Grafana確認は1回
    log_processing(log_directory, uid, f"BL-SWトラフィック確認開始: area={area}")
    try:
        ok = blsw_traffic_check.check_area(
            area,
            log=lambda m, _ld=log_directory, _u=uid: log_detail(_ld, _u, f"[BLSW] {m}"),
        )
    except blsw_traffic_check.GrafanaCheckError as e:
        log_processing(log_directory, uid, f"BL-SWトラフィック確認 判定不能: area={area}")
        log_detail(log_directory, uid, f"[BLSW] {area}: 判定不能 {type(e).__name__}: {e}")
        for h in targets:
            update_node_status(log_directory, uid, h,
                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                f"{h}のBL-SWトラフィック確認不可(判定不能)")
        return

    if ok:
        log_processing(log_directory, uid, f"BL-SWトラフィック確認 OK: area={area}")
    else:
        log_processing(log_directory, uid, f"BL-SWトラフィック確認 NG: area={area}")
        for h in targets:
            update_node_status(log_directory, uid, h,
                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                f"{h}のBL-SWトラフィック確認NG")


def generate_shutdown_files(ports, run_directory, hostname):
    shutdown_file = os.path.join(run_directory, f"{hostname}_shutdown.json")
    noshut_file = os.path.join(run_directory, f"{hostname}_noshut.json")

    with open(shutdown_file, "w") as f:
        f.write(
            '{"polUni":{"attributes":{"dn":"uni"},"children":[\n'
            '{"fabricInst":{"attributes":{"dn":"uni/fabric"},"children":[\n'
            '{"fabricOOServicePol":{"attributes":{"dn":"uni/fabric/outofsvc"},"children":[\n'
        )
        for port in ports:
            f.write(
                f'{{"fabricRsOosPath":{{"attributes":{{"lc":"blacklist","tDn":"{port}"}}}}}},\n'
            )
    replace_characters(shutdown_file, ",", "]}}]}}]}}")

    with open(noshut_file, "w") as f:
        f.write(
            '{"polUni":{"attributes":{"dn":"uni"},"children":[\n'
            '{"fabricInst":{"attributes":{"dn":"uni/fabric"},"children":[\n'
            '{"fabricOOServicePol":{"attributes":{"dn":"uni/fabric/outofsvc"},"children":[\n'
        )
        for port in ports:
            f.write(
                f'{{"fabricRsOosPath":{{"attributes":{{"status":"deleted","tDn":"{port}"}}}}}},\n'
            )
    replace_characters(noshut_file, ",", "]}}]}}]}}")

    return shutdown_file, noshut_file


def create_leaf_shutdown(token, apic_ip, hostname, node_id, pod_id, uid):
    dn_list, spine_dn_list = get_leaf_ports(token, apic_ip, node_id, pod_id)
    ports = sorted([transform_string(p) for p in dn_list], key=extract_key)
    run_directory = os.path.join(script_directory, "run", uid, hostname)
    os.makedirs(run_directory, exist_ok=True)
    if not dn_list:
        shutdown_file = False
    else:
        shutdown_file, noshut_file = generate_shutdown_files(
            ports, run_directory, hostname
        )
        split_file_by_lines(run_directory, f"{hostname}_noshut.json", 5)
    return shutdown_file, dn_list, spine_dn_list


def create_spine_shutdown(token, apic_ip, hostname, node_id, pod_id, uid):
    apic_leaf = apic_leafs.apic_leafs[hostname][0][0]["apic_leaf"]
    time.sleep(5)
    apic_leaf_hostname = get_apic_leaf_hostname(token, apic_ip, apic_leaf)

    dn_list = get_spine_ports(token, apic_ip, node_id, pod_id, apic_leaf)
    spine_apic_leaf_dn_list = {leaf for leaf in dn_list if apic_leaf in leaf}
    spine_dn_list = {leaf for leaf in dn_list if apic_leaf not in leaf}

    ports = sorted([transform_string(p) for p in spine_dn_list], key=extract_key)

    run_directory = os.path.join(script_directory, "run", uid, hostname)
    os.makedirs(run_directory, exist_ok=True)

    shutdown_file, noshut_file = generate_shutdown_files(ports, run_directory, hostname)
    split_file_by_lines(run_directory, f"{hostname}_noshut.json", 5)

    return (
        shutdown_file,
        dn_list,
        spine_dn_list,
        spine_apic_leaf_dn_list,
        apic_leaf_hostname,
    )


def create_apic_shutdown(token, apic_ip, hostname, node_id, pod_id, uid):

    try:
        mapping = apic_leafs.apic_leafs[hostname]
    except KeyError:
        raise KeyError(f"{hostname} not found in apic_leafs mapping")

    apic_leaf_id = mapping[0][0].get("apic_leaf", "").strip()  # kept for context/logs
    node_port_entries = mapping[1]  # [{"node":"1701","ports":["eth1/48"]}, ...]

    raw_dn_list = get_apic_ports(token, apic_ip, node_id, pod_id, up_only=True) or []

    transformed_all = {transform_string(dn) for dn in raw_dn_list}

    expected_transformed = set()
    for ent in node_port_entries:
        n = (ent.get("node") or "").strip()
        for p in ent.get("ports", []):
            p = (p or "").strip()
            if n and p:
                expected_transformed.add(
                    f"topology/pod-{pod_id}/paths-{n}/pathep-[{p}]"
                )

    apic_leaf_dn_list = {dn for dn in transformed_all if dn in expected_transformed}
    other_apic_leaf_dn_list = {
        dn for dn in transformed_all if dn not in expected_transformed
    }

    ports = sorted(apic_leaf_dn_list, key=extract_key)

    run_directory = os.path.join(script_directory, "run", uid, hostname)
    os.makedirs(run_directory, exist_ok=True)

    shutdown_file, noshut_file = generate_shutdown_files(ports, run_directory, "apic")
    return (
        shutdown_file,
        list(transformed_all),
        list(apic_leaf_dn_list),
        list(other_apic_leaf_dn_list),
    )


if __name__ == "__main__":
    main()