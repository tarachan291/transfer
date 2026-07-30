import argparse
import os
import uuid
import time
from datetime import datetime
import re
import config
import blsw_traffic_check
import vpc_peer_check
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


### FOR TEST ###
"""def reload_node(
    token, apic_ip, node_id, pod_id, log_directory=None, uid=None, retries=3, backoff=5
):
    # Change later
    print("Reloading Node")
    print(f"{apic_ip}, {node_id}, {pod_id}, {log_directory}, {uid}")
    return True"""


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


def main():
    # ==== 入口処理: 引数を受け取り、最低限のバリデーションを実施 ====
    parser = argparse.ArgumentParser(description="comm_decomm tool")
    parser.add_argument("--target_node", help="target node")
    parser.add_argument("--pid", help="PID")
    parser.add_argument("--scenario_id", help="commission, decommission or register")
    #parser.add_argument("--type", help="leaf or spine")
    parser.add_argument("--order_group", help="order group")
    args = parser.parse_args()

    hostname = args.target_nodes or []
    uid = args.order_group  # or str(uuid.uuid4())
    scenario = args.scenario_id
    node_type = args.type
    pid = args.pid  # or str(uuid.uuid4())

    #commands_directory = f"{script_directory}/commands/"

    log_directory = f"{script_directory}/log/{uid}"
    scenario_directory = f"{log_directory}/{scenario}"

    status_path = f"{log_directory}/{pid}_status.json"
    main_log_path = f"{log_directory}/{pid}.log"
    processing_log = f"{log_directory}/{pid}_processing.log"
    detail_log = f"{log_directory}/{pid}_detail.log"

    errors = []

    if not hostname:
        errors.append("target_node が指定されていません。")

    if scenario not in ("commission", "decommission", "register"):
        errors.append("scenario_id が不明または指定されていません。")

    #if node_type not in ("leaf", "spine"):
    #    errors.append("type が不正または未指定です。")

    if not uid:
        errors.append("order_group が指定されていません。")

    if not pid:
        errors.append("PID が指定されていません。")

    #if hostnames and node_type in ("leaf", "spine"):
    #    """if not all(
    #        ("Leaf" in h if node_type == "leaf" else "SpSw" in h) for h in hostnames
    #    ):"""
    #    if not all(
    #        (
    #            ("Leaf" in h or "leaf" in h) if node_type == "leaf" else "SpSw" in h
    #        ) for h in hostnames
    #    ):
    #        errors.append("Node type と hostname の命名規制が一致しません。")

    #if node_type == "spine" and len(hostnames) != 1:
    #    errors.append("type=spine の場合、target_nodes は1ノードのみ指定してください。")

    if errors:
        msg = " / ".join(errors)
        print(f"{timestamp()} {msg}")
        set_client_error_status(
            log_directory,
            pid,
            hostname,
            msg,
            code=result_code.STATUS_CODE_CLIENT_ERROR,
        )
        sys.exit(1)

    if scenario == "decommission":
        os.makedirs(log_directory, exist_ok=True)
        os.makedirs(scenario_directory, exist_ok=True)

    if scenario == "register":
        os.makedirs(log_directory, exist_ok=True)
        os.makedirs(scenario_directory, exist_ok=True)

    elif scenario == "commission":
        if not os.path.exists(log_directory):
            msg = f"指定された order_group '{uid}' が存在しません。"
            print(f"{timestamp()} {msg}")
            set_client_error_status(
                log_directory,
                pid,
                hostname,
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
            hostname,
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
        for hostname in (hostname or [])
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

    valid_host = ""

    if hostname_exists(hostname):
        valid_host = hostname

    if not valid_host:
        msg = "指定された hostname が許可リストに含まれていません。"
        print(f"{timestamp()} {msg}")
        log_processing(log_directory, pid, msg)
        set_client_error_status(
            log_directory,
            pid,
            hostname,
            msg,
            code=result_code.HOSTNAME_NOT_ALLOWED_CLIENT_ERROR,
        )
        sys.exit(1)

    #if len(valid_hosts) != len(hostnames):
    #    missing = [h for h in hostnames if h not in valid_hosts]
    #    msg = "存在しない hostname があります: " + ", ".join(missing)
    #    print(f"{timestamp()} {msg}")
    #    log_processing(log_directory, pid, msg)
    #    set_client_error_status(
    #        log_directory,
    #        pid,
    #        hostnames,
    #        msg,
    #        code=result_code.HOSTNAME_NOT_ALLOWED_CLIENT_ERROR,
    #    )
    #    sys.exit(1)

    hostname = valid_host

    step = 1
    steps = 3 if node_type == "leaf" else 4
    action = "ノード切り離し" if scenario == "disable" else "ノード組み込み"

    # ==== STEP1: APIC への接続確認 ====
    #log_step(main_log_path, f"{action}:{node_type.capitalize()} START")
    #log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 START")

    try:
        token_node = random.choice(hostname)
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
        #log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 ERROR")
        #log_step(main_log_path, f"{action}:{node_type.capitalize()} ERROR")
        fail_all_and_exit(log_directory, pid, hostname, "APIC接続失敗")

    #log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 END")
    #step = step + 1

    if scenario == "decommission":
        
        log_processing(
            log_directory,
            pid,
            f"{hostname}: 処理開始 (scenario=disable, type={node_type})",
        )

        try:
            token_node = random.choice(hostname)
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
            #log_step(main_log_path, f"[STEP{step}/{steps}]事前確認 ERROR")
            #log_step(main_log_path, f"{action}:{node_type.capitalize()} ERROR")
            update_node_status(
                log_directory,
                pid,
                hostname,
                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                f"{hostname}: APIC接続失敗",
            )

        node_id, pod_id = get_hostname_info(hostname, apic_ip, apic, token)

        if not node_id or not pod_id:
            #log_step(main_log_path, f"[STEP{step}/{steps}]ノード切り離し ERROR")
            #log_step(
            #    main_log_path, f"ノード切り離し:{node_type.capitalize()} ERROR"
            #)
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


    elif scenario == "commission":
        

    elif scenario == "register":