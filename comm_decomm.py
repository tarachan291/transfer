import argparse
import os
import time
from datetime import datetime
import re
import config
import random
import json
import result_code
import sys
import requests
import psycopg2
import traceback
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# このスクリプトは、APIC に対する ACI ノード 1 台のデコミッション・
# コミッション・新規登録を自動化し、投入後の状態確認と
# ログ・ステータスファイルの生成までを実施するユーティリティ。
# main() で引数を受け取り、指定された 1 ノードに対してシナリオを実行する。
#
# シナリオと投入先 MO:
#   decommission : uni/fabric/outofsvc          fabricRsDecommissionNode (created,modified)
#   commission   : uni/fabric/outofsvc          fabricRsDecommissionNode (deleted)
#   register     : uni/controller/nodeidentpol  fabricNodeIdentP         (created)
#
# デコミッション実行時に node_id / pod_id / serial / role / model / version /
# Forwarding Scale Profile を run/{order_group}/{hostname}_nodeinfo.json に、
# 各ポートのトランシーバ情報を同 _transceivers_before.json に保存し、コミッション / 新規登録は
# 同じ order_group からそれを読み込む（APIC から消えた後でも値を引き継げる）。
# したがってコミッション / 新規登録は、デコミッション済みの order_group を
# 指定することが前提。order_group 自体が無ければクライアントエラー、
# order_group はあるがノード情報が無ければサーバエラーで終了する。
# register の serial だけは筐体交換で変わるため --serial-number で受け取る。
#
# 投入 payload はファイルを介さず、ツール内で組み立てて直接 POST する。
# 投入内容は {pid}_detail.log に記録されるため、事後の追跡はそちらを参照する。

psql_host = config.PSQL_HOST
psql_db = config.PSQL_DB
psql_user = config.PSQL_USER
psql_password = config.PSQL_PASSWORD

apic_username = config.USERNAME
apic_password = config.PASSWORD

protocol = config.PROTOCOL

post_sleep_interval = getattr(config, "POST_FILE_SLEEP_INTERVAL", 5)

script_directory = os.path.dirname(os.path.abspath(__file__))

# シナリオ定義（action 名 / 投入先 MO / 投入後に期待する fabricSt）
SCENARIO_SPEC = {
    "decommission": {
        "action": "デコミッション",
        "mo_dn": "uni/fabric/outofsvc",
        "desired_active": False,
    },
    "commission": {
        "action": "コミッション",
        "mo_dn": "uni/fabric/outofsvc",
        "desired_active": True,
    },
    "register": {
        "action": "新規登録",
        "mo_dn": "uni/controller/nodeidentpol",
        "desired_active": True,
    },
}


def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


def fail_and_exit(
    log_directory,
    uid,
    hostname,
    message,
    code=result_code.EACH_STATUS_CODE_SERVER_ERROR,
):
    update_node_status(log_directory, uid, hostname, code, f"{hostname}: {message}")
    finalize_status(log_directory, uid)
    sys.exit(1)


def finalize_status(log_directory, uid):
    path = f"{log_directory}/{uid}_status.json"
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
    log_directory, uid, hostname, message, code=result_code.STATUS_CODE_CLIENT_ERROR
):
    os.makedirs(log_directory, exist_ok=True)

    json_nodes = (
        [
            {
                "target_node": hostname,
                "each_status_code": code,
                "message": f"{hostname}: {message}",
            }
        ]
        if hostname
        else []
    )

    json_data_structure = {
        "status_code": code,
        "message": message,
        "results": json_nodes,
    }

    with open(f"{log_directory}/{uid}_status.json", "w", encoding="utf-8") as f:
        json.dump(json_data_structure, f, ensure_ascii=False, indent=4)


def update_node_status(log_directory, uid, target_node, status_code, message):
    path = f"{log_directory}/{uid}_status.json"
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
        print(f"{timestamp()} WARNING: Node '{target_node}' not found in status.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# ============================================================
# APIC 接続 / DB アクセス
# ============================================================


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
            http_response = requests.get(
                url, proxies={"http": None, "https": None}, verify=False, timeout=10
            )
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
        raise RuntimeError(f"{timestamp()} No reachable APIC IPs found.")

    return random.choice(successful_apic_ips)


### FOR TEST ENVIRONMENT ###
"""def hostname_exists(hostname):
    hsts = ["tdqntys1-Leaf705", "tdqntys1-Leaf706", "tdqntys1-SpSw05", "tdqntys1-SpSw06"]
    return hostname in hsts"""
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


# ============================================================
# APIC 情報取得
# ============================================================


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
        print(f"{timestamp()} Failed to get host info. {e}")
        return None, None


def get_fabric_node_info(token, apic_ip, hostname):
    """fabricNode から node_id / pod_id / serial / role / model / version / fabricSt を取得する。

    decommission 済みノードは topSystem から消えるため、こちらを優先して使う。
    removeFromController=false であれば decommission 後も fabricNode は残る。
    """
    fabricNode_url = f'{protocol}://{apic_ip}/api/node/class/fabricNode.json?query-target-filter=eq(fabricNode.name,"{hostname}")'
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(fabricNode_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        if not imdata:
            return None

        attrs = imdata[0]["fabricNode"]["attributes"]
        dn = attrs.get("dn", "")
        pod_match = re.search(r"pod-(\d+)", dn)

        return {
            "hostname": hostname,
            "node_id": attrs.get("id", ""),
            "pod_id": pod_match.group(1) if pod_match else "1",
            "serial": attrs.get("serial", ""),
            "role": attrs.get("role", ""),
            "model": attrs.get("model", ""),
            "version": attrs.get("version", ""),
            "fabric_st": attrs.get("fabricSt", ""),
            "dn": dn,
        }
    except Exception as e:
        print(f"{timestamp()} Failed to get fabricNode info. {e}")
        return None


def node_info_path(uid, hostname):
    """デコミッション時のノード情報を保存するパス（order_group 単位）。"""
    return os.path.join(script_directory, "run", uid, f"{hostname}_nodeinfo.json")


def save_node_info(uid, hostname, info, log_directory, pid):
    """デコミッション時のノード情報を JSON で保存する。

    コミッション / 新規登録は同じ order_group からこれを読み込むため、
    保存できない場合は復旧手段が無くなる。失敗時は None を返し、呼び出し側で
    投入前に異常終了させること。
    """
    path = node_info_path(uid, hostname)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=4)
        log_processing(log_directory, pid, f"{hostname}: ノード情報を保存")
        log_detail(log_directory, pid, f"{hostname}: nodeinfo={path} {info}")
        return path
    except Exception as e:
        log_processing(log_directory, pid, f"{hostname}: ノード情報の保存に失敗")
        log_detail(log_directory, pid, f"{hostname}: 保存例外 {type(e).__name__}: {e}")
        return None


def remove_node_info(uid, hostname, log_directory, pid):
    """投入に失敗した場合に、保存済みノード情報を取り消す。"""
    path = node_info_path(uid, hostname)
    try:
        if os.path.exists(path):
            os.remove(path)
            log_processing(log_directory, pid, f"{hostname}: ノード情報を取り消し")
    except Exception as e:
        log_detail(log_directory, pid, f"{hostname}: 取消例外 {type(e).__name__}: {e}")


def load_node_info(uid, hostname, log_directory, pid):
    """同じ order_group のデコミッション時ノード情報を読み込む。

    コミッション / 新規登録はこの情報が前提のため、読めない場合は例外を送出する。
    """
    path = node_info_path(uid, hostname)
    if not os.path.exists(path):
        log_detail(log_directory, pid, f"{hostname}: nodeinfo不在 path={path}")
        raise RuntimeError(f"ノード情報がありません: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
    except Exception as e:
        log_detail(log_directory, pid, f"{hostname}: 読込例外 {type(e).__name__}: {e}")
        raise RuntimeError(f"ノード情報の読込に失敗しました: {path}")

    if not info.get("node_id"):
        log_detail(log_directory, pid, f"{hostname}: nodeinfo={info}")
        raise RuntimeError(f"ノード情報に node_id がありません: {path}")

    log_processing(log_directory, pid, f"{hostname}: デコミッション時のノード情報を使用")
    log_detail(log_directory, pid, f"{hostname}: nodeinfo={path} {info}")
    return info


def resolve_node_info(token, apic_ip, apic, hostname, node_type, serial=""):
    """投入に必要なノード情報を確定する。

    node_id / pod_id / role は fabricNode → topSystem の順に取得する。
    serial だけは APIC から引けるのが登録済みノードに限られるため、
    --serial-number で渡された値を使う（未指定なら fabricNode の値）。
    """
    info = get_fabric_node_info(token, apic_ip, hostname)

    if not info or not info.get("node_id"):
        node_id, pod_id = get_hostname_info(hostname, apic_ip, apic, token)
        info = {
            "hostname": hostname,
            "node_id": node_id or "",
            "pod_id": pod_id or "",
            "serial": "",
            "role": "",
            "model": "",
            "version": "",
            "fabric_st": "",
        }

    # serial は引数指定を優先
    if serial:
        info["serial"] = serial

    if not info.get("role"):
        info["role"] = node_type
    if not info.get("pod_id"):
        info["pod_id"] = "1"

    return info


def get_fwd_scale_profile(token, apic_ip, node_id, pod_id):
    """topoctrlFwdScaleProf から Forwarding Scale Profile（profType）を取得する。

    取得できない場合は空文字を返す（記録目的のため、処理は継続する）。
    """
    url = (
        f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{node_id}/"
        "topoctrlFwdScaleProf.json"
    )
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        if not imdata or "topoctrlFwdScaleProf" not in imdata[0]:
            return ""

        return imdata[0]["topoctrlFwdScaleProf"]["attributes"].get("profType", "")
    except Exception as e:
        print(f"{timestamp()} Failed to get topoctrlFwdScaleProf. {e}")
        return ""


def get_transceivers(token, apic_ip, node_id, pod_id):
    """ethpmFcot から各ポートのトランシーバ情報を取得する。

    戻り値は {ポート名: {"typeName": ..., "guiSN": ...}} の辞書。
    取得できない場合は None（空の辞書とは区別する）。
    """
    url = (
        f"{protocol}://{apic_ip}/api/node/class/topology/pod-{pod_id}/node-{node_id}/"
        'ethpmFcot.json?query-target-filter=eq(ethpmFcot.state,"inserted")'
    )
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]
    except Exception as e:
        print(f"{timestamp()} Failed to get ethpmFcot. {e}")
        return None

    transceivers = {}
    for item in imdata:
        if "ethpmFcot" not in item:
            continue

        attrs = item["ethpmFcot"].get("attributes", {})

        # クエリ側でも絞っているが、念のため未実装ポートを除外する
        if attrs.get("state", "") != "inserted":
            continue

        dn = attrs.get("dn", "")
        port_match = re.search(r"phys-\[([^\]]+)\]", dn)
        port = port_match.group(1) if port_match else dn

        transceivers[port] = {
            "typeName": attrs.get("typeName", "").strip(),
            "guiSN": attrs.get("guiSN", "").strip(),
        }

    return transceivers


def transceiver_path(uid, hostname, phase):
    """トランシーバ情報の保存パス（phase は before / after / diff）。"""
    return os.path.join(
        script_directory, "run", uid, f"{hostname}_transceivers_{phase}.json"
    )


def save_transceivers(uid, hostname, phase, data, log_directory, pid):
    """トランシーバ情報を JSON で保存する。失敗時は None を返す。"""
    path = transceiver_path(uid, hostname, phase)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4, sort_keys=True)
        log_processing(
            log_directory,
            pid,
            f"{hostname}: トランシーバ情報を保存 ({phase}, {len(data)}ポート)",
        )
        log_detail(log_directory, pid, f"{hostname}: transceivers({phase})={path}")
        return path
    except Exception as e:
        log_processing(
            log_directory, pid, f"{hostname}: トランシーバ情報の保存に失敗 ({phase})"
        )
        log_detail(log_directory, pid, f"{hostname}: 保存例外 {type(e).__name__}: {e}")
        return None


def load_transceivers(uid, hostname, phase, log_directory, pid):
    """保存済みのトランシーバ情報を読み込む。無ければ None。"""
    path = transceiver_path(uid, hostname, phase)
    if not os.path.exists(path):
        log_detail(log_directory, pid, f"{hostname}: transceivers不在 path={path}")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_detail(log_directory, pid, f"{hostname}: 読込例外 {type(e).__name__}: {e}")
        return None


def compare_transceivers(before, after):
    """事前・事後のトランシーバ情報を比較し、差分の一覧を返す。

    比較対象は typeName（種別）と guiSN（シリアル番号）。
    """
    diffs = []

    for port in sorted(set(before) | set(after)):
        b = before.get(port)
        a = after.get(port)

        if b and not a:
            diffs.append(
                {"port": port, "reason": "欠落", "before": b, "after": None}
            )
        elif a and not b:
            diffs.append(
                {"port": port, "reason": "増設", "before": None, "after": a}
            )
        elif b.get("guiSN", "") != a.get("guiSN", ""):
            diffs.append(
                {"port": port, "reason": "シリアル相違", "before": b, "after": a}
            )
        elif b.get("typeName", "") != a.get("typeName", ""):
            diffs.append(
                {"port": port, "reason": "種別相違", "before": b, "after": a}
            )

    return diffs


def verify_node_info(token, apic_ip, hostname, expected, node_type, log_directory, pid):
    """登録後のノードが、デコミッション時に保持した構成と一致するか確認する。

    不一致だった項目のリストを返す（空なら一致）。
    serial は筐体交換で変わる前提のため比較対象に含めない。
    """
    current = get_fabric_node_info(token, apic_ip, hostname)

    if not current:
        log_processing(log_directory, pid, f"{hostname}: fabricNode が取得できません")
        return ["fabricNode が取得できません"]

    current["fwd_scale_prof"] = get_fwd_scale_profile(
        token, apic_ip, current.get("node_id", ""), current.get("pod_id", "")
    )
    current["node_type"] = current.get("role", "")

    log_detail(log_directory, pid, f"{hostname}: 登録後の構成={current}")

    # (項目名, 期待値, 実際の値)
    targets = [
        ("hostname", expected.get("hostname", hostname), current.get("hostname", "")),
        ("node_id", expected.get("node_id", ""), current.get("node_id", "")),
        ("pod_id", expected.get("pod_id", ""), current.get("pod_id", "")),
        ("role", expected.get("role", ""), current.get("role", "")),
        ("node_type", expected.get("node_type", node_type), current.get("node_type", "")),
        ("model", expected.get("model", ""), current.get("model", "")),
        ("version", expected.get("version", ""), current.get("version", "")),
        (
            "fwd_scale_prof",
            expected.get("fwd_scale_prof", ""),
            current.get("fwd_scale_prof", ""),
        ),
    ]

    mismatches = []
    for name, want, got in targets:
        if not want:
            # 保持していない項目は比較しない
            log_processing(log_directory, pid, f"{hostname}: {name} 比較スキップ（保持値なし）")
            continue

        if str(want) == str(got):
            log_processing(log_directory, pid, f"{hostname}: {name} 一致 ({got})")
        else:
            log_processing(
                log_directory, pid, f"{hostname}: {name} 不一致 (期待={want}, 実際={got})"
            )
            mismatches.append(f"{name}(期待={want}, 実際={got})")

    return mismatches


def get_node_status(token, apic_ip, hostname):
    """fabricSt が active なら True。MO 自体が無い場合も False。"""
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
        print(f"{timestamp()} Failed to get node status. {e}")
        return False


def get_pending_node(token, apic_ip, serial):
    """登録待ちノード（dhcpClient）を取得する。

    Fabric Membership の Nodes Pending Registration に相当する。
    未接続・シリアル誤りの場合は APIC 側にエントリが無いため None を返す。
    登録済みノードも同じクラスに残るので、判定は呼び出し側で nodeId を見る。
    """
    pending_url = f'{protocol}://{apic_ip}/api/node/class/dhcpClient.json?query-target-filter=eq(dhcpClient.id,"{serial}")'
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(pending_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        if not imdata:
            return None

        return imdata[0]["dhcpClient"]["attributes"]
    except Exception as e:
        print(f"{timestamp()} Failed to get dhcpClient. {e}")
        return None


def get_node_ident(token, apic_ip, serial):
    """fabricNodeIdentP（ノードID登録）の存在確認。登録済みなら attributes を返す。"""
    ident_url = f'{protocol}://{apic_ip}/api/node/class/fabricNodeIdentP.json?query-target-filter=eq(fabricNodeIdentP.serial,"{serial}")'
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})
    try:
        response = session.get(ident_url, proxies={"http": None, "https": None})
        response.raise_for_status()
        imdata = response.json()["imdata"]

        if not imdata:
            return None

        return imdata[0]["fabricNodeIdentP"]["attributes"]
    except Exception as e:
        print(f"{timestamp()} Failed to get fabricNodeIdentP. {e}")
        return None


def wait_for_node_status(hostname, desired_status=True, timeout=1800, interval=60):
    """fabricSt が desired_status（True=active / False=非active or 不在）になるまで待つ。"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            new_token, new_apic_ip, _ = get_token_from_random_node(hostname)
            current = get_node_status(new_token, new_apic_ip, hostname)
            print(f"[DEBUG] {hostname} current={current}, desired={desired_status}")
            if current == desired_status:
                return True
        except Exception as e:
            print(f"[DEBUG] wait_for_node_status retry: {type(e).__name__}: {e}")
        time.sleep(interval)
    return False


# ============================================================
# POST 系（payload はツール内で組み立てて直接投入する）
# ============================================================


def build_decommission_payload(node_id, pod_id, remove_from_controller=False):
    return {
        "fabricRsDecommissionNode": {
            "attributes": {
                "tDn": f"topology/pod-{pod_id}/node-{node_id}",
                "status": "created,modified",
                "removeFromController": "true" if remove_from_controller else "false",
            }
        }
    }


def build_commission_payload(node_id, pod_id):
    return {
        "fabricRsDecommissionNode": {
            "attributes": {
                "tDn": f"topology/pod-{pod_id}/node-{node_id}",
                "status": "deleted",
            }
        }
    }


def build_register_payload(hostname, serial, node_id, pod_id, role):
    attributes = {
        "serial": serial,
        "nodeId": str(node_id),
        "name": hostname,
        "podId": str(pod_id),
        "status": "created",
    }
    if role:
        attributes["role"] = role
    return {"fabricNodeIdentP": {"attributes": attributes}}


def build_payload(scenario, info, hostname, remove_from_controller=False):
    if scenario == "decommission":
        return build_decommission_payload(
            info["node_id"], info["pod_id"], remove_from_controller
        )
    if scenario == "commission":
        return build_commission_payload(info["node_id"], info["pod_id"])
    return build_register_payload(
        hostname,
        info.get("serial", ""),
        info["node_id"],
        info.get("pod_id", "1"),
        info.get("role", ""),
    )


### FOR TEST ###
"""def post_mo(token, apic_ip, mo_dn, payload,
            log_directory=None, uid=None, retries=3, backoff=5):
    # Change later
    print(f"POST {mo_dn}: {json.dumps(payload, ensure_ascii=False)}")
    return True"""
### FOR TEST ###


def post_mo(
    token, apic_ip, mo_dn, payload, log_directory=None, uid=None, retries=3, backoff=5
):
    """任意の MO DN に対して payload（dict）を直接 POST する。

    decommission / commission は uni/fabric/outofsvc、register は
    uni/controller/nodeidentpol が対象になるため、投入先を引数で指定する。
    成功時はレスポンス JSON、最終失敗時は False を返す。
    """
    url = f"{protocol}://{apic_ip}/api/node/mo/{mo_dn}.json"
    session = requests.Session()
    session.verify = False
    session.headers.update(
        {"Cookie": f"APIC-Cookie={token}", "Content-Type": "application/json"}
    )

    log_detail(
        log_directory,
        uid,
        f"POST {url} payload={json.dumps(payload, ensure_ascii=False)}",
    )

    for attempt in range(1, retries + 1):
        resp = None
        try:
            resp = session.post(
                url, json=payload, proxies={"http": None, "https": None}, timeout=20
            )
            resp.raise_for_status()
            log_processing(log_directory, uid, f"POST成功 -> {mo_dn}")
            return resp.json()

        except Exception as e:
            log_processing(
                log_directory, uid, f"POST失敗 {mo_dn} (試行{attempt}/{retries})"
            )

            try:
                body = resp.text
            except Exception:
                body = "NO RESPONSE"

            log_detail(
                log_directory, uid, f"{mo_dn}: {type(e).__name__}: {e} | body: {body}"
            )

            if attempt < retries:
                time.sleep(backoff)
                continue
            else:
                log_processing(log_directory, uid, f"POST最終失敗 {mo_dn}")
                return False


# ============================================================
# 引数ユーティリティ
# ============================================================


def infer_node_type(hostname):
    """--type 未指定時に hostname の命名規則から leaf / spine を推定する。"""
    if not hostname:
        return None
    if "Leaf" in hostname or "leaf" in hostname:
        return "leaf"
    if "SpSw" in hostname or "spine" in hostname:
        return "spine"
    return None


# ============================================================
# main
# ============================================================


def main():
    # ==== 入口処理: 引数を受け取り、最低限のバリデーションを実施 ====
    parser = argparse.ArgumentParser(description="comm_decomm tool")
    parser.add_argument("--target_node", help="target node (1台のみ)")
    parser.add_argument("--pid", help="PID")
    parser.add_argument("--scenario_id", help="commission, decommission or register")
    parser.add_argument("--type", help="leaf or spine")
    parser.add_argument("--order_group", help="order group")
    parser.add_argument(
        "--serial-number",
        "--serial_number",
        dest="serial_number",
        help="ノードのシリアル番号（scenario_id=register では必須）",
    )
    parser.add_argument(
        "--remove_from_controller",
        action="store_true",
        help="decommission 時に APIC から完全に削除する（既定は false）",
    )
    parser.add_argument(
        "--wait_timeout",
        type=int,
        default=1800,
        help="投入後の状態確認タイムアウト秒（0 で待機なし）",
    )
    args = parser.parse_args()

    hostname = (args.target_node or "").strip()
    uid = args.order_group
    scenario = args.scenario_id
    node_type = args.type or infer_node_type(hostname)
    pid = args.pid
    serial_number = (args.serial_number or "").strip()
    remove_from_controller = args.remove_from_controller
    wait_timeout = args.wait_timeout

    log_directory = f"{script_directory}/log/{uid}"
    status_path = f"{log_directory}/{pid}_status.json"
    processing_log = f"{log_directory}/{pid}_processing.log"
    detail_log = f"{log_directory}/{pid}_detail.log"

    errors = []

    if not hostname:
        errors.append("target_node が指定されていません。")

    if "," in hostname or re.search(r"\s", hostname):
        errors.append("target_node は1ノードのみ指定してください。")

    if scenario not in SCENARIO_SPEC:
        errors.append("scenario_id が不明または指定されていません。")

    if node_type not in ("leaf", "spine"):
        errors.append("type が不正または未指定です。")

    if not uid:
        errors.append("order_group が指定されていません。")

    if not pid:
        errors.append("PID が指定されていません。")

    if scenario == "register" and not serial_number:
        errors.append("scenario_id=register では serial-number が必須です。")

    if serial_number and not re.fullmatch(r"[A-Za-z0-9]+", serial_number):
        errors.append("serial-number の形式が不正です。")

    if hostname and node_type in ("leaf", "spine"):
        if not (
            ("Leaf" in hostname or "leaf" in hostname)
            if node_type == "leaf"
            else "SpSw" in hostname
        ):
            errors.append("Node type と hostname の命名規制が一致しません。")

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

    spec = SCENARIO_SPEC[scenario]

    # コミッション / 新規登録はデコミッション済みの order_group が前提
    if scenario in ("commission", "register"):
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

    os.makedirs(log_directory, exist_ok=True)

    if any(
        os.path.exists(p)
        for p in [status_path, processing_log, detail_log]
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

    json_data_structure = {
        "status_code": result_code.STATUS_CODE_SUCCESS,
        "message": "処理中",
        "results": [
            {
                "target_node": hostname,
                "each_status_code": result_code.EACH_STATUS_CODE_IN_PROGRESS,
                "message": f"{hostname}の処理中",
            }
        ],
    }

    with open(status_path, "w") as f:
        json.dump(json_data_structure, f, ensure_ascii=False, indent=4)

    for name in ["processing", "detail"]:
        open(f"{log_directory}/{pid}_{name}.log", "w").close()

    log_processing(
        log_directory,
        pid,
        f"ログ初期化: {processing_log}, {detail_log}",
    )

    if not hostname_exists(hostname):
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

    # ノード情報（デコミッション時に保存）が無ければ、この時点で異常終了する
    if scenario in ("commission", "register"):
        if not os.path.exists(node_info_path(uid, hostname)):
            msg = f"ID不明: {uid}（{hostname} のノード情報がありません）"
            print(f"{timestamp()} {msg}")
            log_processing(log_directory, pid, msg)
            log_detail(
                log_directory, pid, f"{hostname}: nodeinfo不在 path={node_info_path(uid, hostname)}"
            )
            fail_and_exit(log_directory, pid, hostname, msg)

    action = spec["action"]

    # ==== STEP1: APIC 接続とノード情報の確定 ====
    log_processing(
        log_directory, pid, f"{hostname}: 処理開始 (scenario={scenario}, type={node_type})"
    )

    try:
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
        fail_and_exit(log_directory, pid, hostname, "APIC接続失敗")

    try:
        # コミッション / 新規登録は、同じ order_group のデコミッション時情報を使う
        if scenario in ("commission", "register"):
            info = load_node_info(uid, hostname, log_directory, pid)
            if serial_number:
                info["serial"] = serial_number
            if not info.get("role"):
                info["role"] = node_type
            if not info.get("pod_id"):
                info["pod_id"] = "1"
        else:
            info = resolve_node_info(
                token, apic_ip, apic, hostname, node_type, serial=serial_number
            )

        if not info.get("node_id"):
            log_processing(log_directory, pid, f"{hostname}: node_id/pod_id 取得失敗")
            log_detail(
                log_directory,
                pid,
                f"{hostname}: ノード情報取得失敗 apic_ip={apic_ip}, apic={apic}",
            )
            raise RuntimeError("node_id / pod_id が取得できません")

        # 新規登録の事前確認: 二重登録と、登録待ちノードの実在・ロールを検証する
        if scenario == "register":
            registered = get_node_ident(token, apic_ip, info["serial"])
            if registered:
                log_processing(
                    log_directory,
                    pid,
                    f"{hostname}: serial={info['serial']} は既に登録済み",
                )
                log_detail(
                    log_directory, pid, f"{hostname}: 既存 fabricNodeIdentP={registered}"
                )
                raise RuntimeError(
                    f"serial={info['serial']} は既に "
                    f"nodeId={registered.get('nodeId')} / name={registered.get('name')} "
                    f"として登録されています"
                )

            # 指定シリアルが実際に登録待ち（dhcpClient）として見えているかを確認する
            pending = get_pending_node(token, apic_ip, info["serial"])

            if not pending:
                log_processing(
                    log_directory,
                    pid,
                    f"{hostname}: serial={info['serial']} は登録待ちノードに存在しません",
                )
                raise RuntimeError(
                    f"serial={info['serial']} は登録待ちノードに存在しません"
                    "（未接続、またはシリアル誤り）"
                )

            pending_node_id = pending.get("nodeId", "")
            if pending_node_id not in ("", "0"):
                log_processing(
                    log_directory,
                    pid,
                    f"{hostname}: serial={info['serial']} は既にnodeId={pending_node_id}",
                )
                log_detail(log_directory, pid, f"{hostname}: dhcpClient={pending}")
                raise RuntimeError(
                    f"serial={info['serial']} は既に nodeId={pending_node_id} が"
                    "割り当てられています"
                )

            pending_role = pending.get("nodeRole", "")
            if pending_role and pending_role != node_type:
                log_processing(
                    log_directory,
                    pid,
                    f"{hostname}: ロール不一致 (指定={node_type}, 実機={pending_role})",
                )
                raise RuntimeError(
                    f"serial={info['serial']} のロールは {pending_role} です"
                    f"（指定は {node_type}）"
                )

            log_processing(
                log_directory,
                pid,
                f"{hostname}: 登録待ちノードを確認 "
                f"(serial={info['serial']}, role={pending_role}, "
                f"model={pending.get('model', '')})",
            )
            log_detail(log_directory, pid, f"{hostname}: dhcpClient={pending}")

        log_detail(
            log_directory,
            pid,
            f"{hostname}: node_id={info['node_id']}, pod_id={info['pod_id']}, "
            f"serial={info.get('serial')}, role={info.get('role')}, "
            f"model={info.get('model')}, version={info.get('version')}, "
            f"fabricSt={info.get('fabric_st')}",
        )

    except Exception as e:
        log_detail(
            log_directory,
            pid,
            f"{hostname}: 例外 {type(e).__name__}: {e}\n{traceback.format_exc()}",
        )
        print(f"{timestamp()} {e}")
        fail_and_exit(log_directory, pid, hostname, str(e))

    # ==== STEP2: API 投入 ====

    try:
        payload = build_payload(scenario, info, hostname, remove_from_controller)

        # ノード情報は投入前に保存する。
        # 投入後に保存が失敗すると「切り離し済みだが復旧情報が無い」状態になるため、
        # 保存できない場合は APIC へ何も投入せずに異常終了させる。
        if scenario == "decommission":
            # 切り離し前の構成を記録として残す（取得できなくても投入は継続する）
            fwd_scale_prof = get_fwd_scale_profile(
                token, apic_ip, info["node_id"], info["pod_id"]
            )
            log_processing(
                log_directory,
                pid,
                f"{hostname}: model={info.get('model', '')}, "
                f"version={info.get('version', '')}, "
                f"fwd_scale_prof={fwd_scale_prof or '取得不可'}",
            )

            saved = save_node_info(
                uid,
                hostname,
                {
                    "hostname": hostname,
                    "node_id": info["node_id"],
                    "pod_id": info["pod_id"],
                    "serial": info.get("serial", ""),
                    "role": info.get("role", ""),
                    "model": info.get("model", ""),
                    "version": info.get("version", ""),
                    "fwd_scale_prof": fwd_scale_prof,
                    "node_type": node_type,
                    "order_group": uid,
                    "pid": pid,
                    "remove_from_controller": bool(remove_from_controller),
                    "decommissioned_at": timestamp(),
                },
                log_directory,
                pid,
            )
            if not saved:
                raise RuntimeError("ノード情報の保存に失敗しました（投入は未実施）")

            # 切り離し前のトランシーバ情報を保存（新規登録の事後比較に使う）
            transceivers = get_transceivers(
                token, apic_ip, info["node_id"], info["pod_id"]
            )
            if transceivers is None:
                log_processing(
                    log_directory,
                    pid,
                    f"{hostname}: トランシーバ情報を取得できません（比較は実施されません）",
                )
            else:
                save_transceivers(
                    uid, hostname, "before", transceivers, log_directory, pid
                )

        log_processing(log_directory, pid, f"{hostname}: {action}投入開始")

        resp = post_mo(
            token,
            apic_ip,
            spec["mo_dn"],
            payload,
            log_directory=log_directory,
            uid=pid,
        )

        if resp is False:
            # 投入していないので、保存したノード情報は残さない
            if scenario == "decommission":
                remove_node_info(uid, hostname, log_directory, pid)
            fail_and_exit(
                log_directory, pid, hostname, f"POST失敗: {spec['mo_dn']}"
            )

        log_processing(log_directory, pid, f"{hostname}: {action}投入完了")

    except Exception as e:
        log_processing(log_directory, pid, f"{hostname}: 例外発生 -> 異常終了")
        log_detail(
            log_directory,
            pid,
            f"{hostname}: 例外 {type(e).__name__}: {e}\n{traceback.format_exc()}",
        )
        print(f"{timestamp()} {e}")
        fail_and_exit(log_directory, pid, hostname, f"{action}異常終了")

    time.sleep(post_sleep_interval)

    # ==== STEP3: 事後確認 ====

    try:
        # register はノードID登録そのものを確認する
        if scenario == "register":
            ident = get_node_ident(token, apic_ip, info["serial"])
            if not ident:
                log_processing(log_directory, pid, f"{hostname}: ノードID登録の確認NG")
                raise RuntimeError("fabricNodeIdentP が確認できません")
            log_processing(log_directory, pid, f"{hostname}: ノードID登録の確認OK")
            log_detail(log_directory, pid, f"{hostname}: fabricNodeIdentP={ident}")

        # fabricSt がシナリオの期待値になるまで待機
        if wait_timeout > 0:
            expected = "非active" if not spec["desired_active"] else "active"
            log_processing(log_directory, pid, f"{hostname}: fabricSt {expected}待機開始")
            reached = wait_for_node_status(
                hostname,
                desired_status=spec["desired_active"],
                timeout=wait_timeout,
                interval=30 if scenario == "decommission" else 60,
            )
            if not reached:
                log_processing(
                    log_directory, pid, f"{hostname}: 状態確認NG（timeout）"
                )
                raise RuntimeError(f"{action}後の状態確認NG（fabricSt {expected}未達）")

            log_processing(log_directory, pid, f"{hostname}: 状態確認OK")

        # 新規登録は、デコミッション時に保持した構成と一致しているかを確認する
        if scenario == "register":
            mismatches = verify_node_info(
                token, apic_ip, hostname, info, node_type, log_directory, pid
            )
            if mismatches:
                raise RuntimeError("構成不一致: " + " / ".join(mismatches))

            log_processing(log_directory, pid, f"{hostname}: 構成一致確認OK")

            # トランシーバの事前・事後比較
            before_tr = load_transceivers(uid, hostname, "before", log_directory, pid)

            if before_tr is None:
                log_processing(
                    log_directory,
                    pid,
                    f"{hostname}: 比較元のトランシーバ情報なし -> 比較スキップ",
                )
            else:
                after_tr = get_transceivers(
                    token, apic_ip, info["node_id"], info["pod_id"]
                )
                if after_tr is None:
                    log_processing(
                        log_directory, pid, f"{hostname}: トランシーバ情報の取得NG"
                    )
                    raise RuntimeError("トランシーバ情報が取得できません")

                save_transceivers(
                    uid, hostname, "after", after_tr, log_directory, pid
                )

                tr_diffs = compare_transceivers(before_tr, after_tr)

                if tr_diffs:
                    save_transceivers(
                        uid, hostname, "diff", tr_diffs, log_directory, pid
                    )
                    for d in tr_diffs:
                        b_sn = (d["before"] or {}).get("guiSN", "-")
                        a_sn = (d["after"] or {}).get("guiSN", "-")
                        log_processing(
                            log_directory,
                            pid,
                            f"{hostname}: {d['port']} {d['reason']} "
                            f"(事前={b_sn}, 事後={a_sn})",
                        )
                    raise RuntimeError(
                        f"トランシーバ不一致 {len(tr_diffs)}件"
                        f"（{transceiver_path(uid, hostname, 'diff')} 参照）"
                    )

                log_processing(
                    log_directory,
                    pid,
                    f"{hostname}: トランシーバ一致確認OK ({len(after_tr)}ポート)",
                )

    except Exception as e:
        log_processing(log_directory, pid, f"{hostname}: 事後確認NG -> 異常終了")
        log_detail(
            log_directory,
            pid,
            f"{hostname}: 例外 {type(e).__name__}: {e}\n{traceback.format_exc()}",
        )
        print(f"{timestamp()} {e}")
        fail_and_exit(log_directory, pid, hostname, f"{action}異常終了: {e}")

    update_node_status(
        log_directory,
        pid,
        hostname,
        result_code.EACH_STATUS_CODE_COMPLETED,
        f"{hostname}の{action}正常終了",
    )


    finalize_status(log_directory, pid)
    log_processing(log_directory, pid, f"{action} 完了")


if __name__ == "__main__":
    main()
