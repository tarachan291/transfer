import argparse
import os
import re
import sys
import json
import random
import subprocess
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import urllib3

import config
import result_code

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# このスクリプトは、指定された Leaf（target_nodes）に対する「冗長 Leaf」を特定し、
# JSON ファイルとして出力するユーティリティ。
#   1) APIC の vPC 明示的保護グループ（fabricExplicitGEp / fabricNodePEp）を参照し、
#      vPC ピアが存在する場合はそれを冗長 Leaf とする。
#   2) vPC を組んでいない Leaf は、PostgreSQL（t_if）の接続先ホスト名（rm_hostname）
#      を突き合わせるロジックで、同一機器を収容している Leaf を冗長 Leaf とする。
# ログ・ステータスファイルの生成は nodeshut_vup.py と同じ設計に揃えている。

psql_host = config.PSQL_HOST
psql_db = config.PSQL_DB
psql_user = config.PSQL_USER
psql_password = config.PSQL_PASSWORD

apic_username = config.USERNAME
apic_password = config.PASSWORD

protocol = config.PROTOCOL

# 並列度・セッションパラメータは CLI ではなく config で調整する
max_workers = getattr(config, "MAX_WORKERS", 4)
psql_work_mem = getattr(config, "PSQL_WORK_MEM", None)
psql_statement_timeout = getattr(config, "PSQL_STATEMENT_TIMEOUT", None)
# 冗長Leaf特定クエリの参照期間（日）。短いほど速い
psql_lookback_days = int(getattr(config, "PSQL_LOOKBACK_DAYS", 7))

# 正常性確認ツール（別ツール）の設定
normalcy_check_enabled = getattr(config, "NORMALCY_CHECK_ENABLED", True)
normalcy_check_path = getattr(
    config, "NORMALCY_CHECK_PATH", "/home/kddi/scripts/normalcy_check/main.py"
)
normalcy_check_timeout = int(getattr(config, "NORMALCY_CHECK_TIMEOUT", 600))

status_json_lock = threading.Lock()
cache_lock = threading.Lock()
# 正常性確認の結果キャッシュ（同じ冗長Leafが複数の対象から参照されても1回だけ実行する）
normalcy_cache = {}
normalcy_cache_lock = threading.Lock()
normalcy_host_locks = {}
log_lock = threading.Lock()

script_directory = os.path.dirname(os.path.abspath(__file__))


# ==========================================================================
# ログ / タイムスタンプ
# ==========================================================================
def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_timestamp():
    return datetime.now().strftime("%Y%m%d%H%M%S")


def log_processing(log_directory, pid, message):
    """処理フローの進捗ログ（人間が追う用）"""
    log_path = f"{log_directory}/{pid}_processing.log"
    with log_lock:
        print(f"{timestamp()} [PROCESSING] {message}")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{timestamp()} [PROCESSING] {message}\n")


def log_detail(log_directory, pid, message):
    """デバッグ用の詳細ログ（調査用の深い情報）"""
    log_path = f"{log_directory}/{pid}_detail.log"
    with log_lock:
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


# ==========================================================================
# ステータスファイル
# ==========================================================================
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


# ==========================================================================
# APIC 接続
# ==========================================================================
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


def check_connection(apic_ips):
    successful_apic_ips = []
    for ip in apic_ips:
        try:
            url = f"{protocol}://{ip}/api/class/topSystem.json"
            http_response = requests.get(
                url,
                proxies={"http": None, "https": None},
                verify=False,
                timeout=10,
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
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raise RuntimeError(f"{now} No reachable APIC IPs found.")

    apic_ip = random.choice(successful_apic_ips)
    return apic_ip


def get_token_from_random_node(hostname):
    apic_ip, apic = apic_select(hostname)
    token = get_token(apic_ip, apic_username, apic_password)
    return token, apic_ip, apic


# ==========================================================================
# PostgreSQL
# ==========================================================================
SESSION_PARAM_PATTERN = re.compile(r"^[0-9]+(kB|MB|GB|ms|s|min)?$", re.IGNORECASE)


def apply_session_params(cur):
    """
    重いクエリ用のセッションパラメータを適用する。
    work_mem を上げるとソート／ハッシュのディスク書き出しを避けられる。
    値は config で指定し、SQL インジェクション防止のため書式を検証する。
    """
    for name, value in (
        ("work_mem", psql_work_mem),
        ("statement_timeout", psql_statement_timeout),
    ):
        if not value:
            continue
        value = str(value)
        if not SESSION_PARAM_PATTERN.match(value):
            print(f"{timestamp()} 不正な {name} の指定を無視します: {value}")
            continue
        cur.execute(f"SET LOCAL {name} = '{value}'")


def fetch_from_psql(psql_host, psql_db, psql_user, psql_password, query, params=None):
    """1列目のみを文字列リストで返す（既存ツールと同じ挙動）。"""
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


def fetch_dict_rows_from_psql(
    psql_host, psql_db, psql_user, psql_password, query, params=None
):
    """全列を dict のリストで返す（冗長Leaf特定クエリ用）。失敗時は例外を送出する。"""
    db_config = {
        "host": f"{psql_host}",
        "database": f"{psql_db}",
        "user": f"{psql_user}",
        "password": f"{psql_password}",
    }

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            apply_session_params(cur)
            if params is not None:
                cur.execute(query, params)
            else:
                cur.execute(query)
            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]


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


# ==========================================================================
# APIC: ノード情報 / vPC 保護グループ
# ==========================================================================
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


def get_topsystem_map(token, apic_ip):
    """node_id -> {hostname, pod_id, role} のマップを取得する。"""
    topSystem_url = f"{protocol}://{apic_ip}/api/node/class/topSystem.json"
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    response = session.get(topSystem_url, proxies={"http": None, "https": None})
    response.raise_for_status()
    imdata = response.json()["imdata"]

    node_map = {}
    for item in imdata:
        attrs = item["topSystem"]["attributes"]
        node_map[str(attrs.get("id"))] = {
            "hostname": attrs.get("name"),
            "pod_id": str(attrs.get("podId")),
            "role": attrs.get("role"),
        }
    return node_map


def get_vpc_groups(token, apic_ip):
    """
    vPC 明示的保護グループ（fabricExplicitGEp）とそのメンバー（fabricNodePEp）を取得する。

    Returns:
        [{"name": ..., "id": ..., "dn": ..., "members": ["101", "102"]}, ...]
    """
    gep_url = (
        f"{protocol}://{apic_ip}/api/node/class/fabricExplicitGEp.json?"
        "rsp-subtree=children&rsp-subtree-class=fabricNodePEp"
    )
    session = requests.Session()
    session.verify = False
    session.headers.update({"Cookie": "APIC-Cookie=" + token})

    response = session.get(gep_url, proxies={"http": None, "https": None})
    response.raise_for_status()
    imdata = response.json()["imdata"]

    groups = []
    for item in imdata:
        gep = item.get("fabricExplicitGEp", {})
        attrs = gep.get("attributes", {})
        members = []
        for child in gep.get("children", []) or []:
            node_pep = child.get("fabricNodePEp")
            if not node_pep:
                continue
            member_id = node_pep.get("attributes", {}).get("id")
            if member_id:
                members.append(str(member_id))
        groups.append(
            {
                "name": attrs.get("name"),
                "id": attrs.get("id"),
                "dn": attrs.get("dn"),
                "members": sorted(members, key=lambda x: int(x) if x.isdigit() else 0),
            }
        )
    return groups


def find_vpc_peers(node_id, vpc_groups, node_map):
    """
    node_id が所属する vPC 保護グループから、自ノード以外のメンバーを返す。

    Returns:
        (peers, group)  peers は [{"hostname","node_id","pod_id","role"}, ...]
                        vPC 未構成なら ([], None)
    """
    node_id = str(node_id)
    for group in vpc_groups:
        if node_id not in group["members"]:
            continue

        peers = []
        for member_id in group["members"]:
            if member_id == node_id:
                continue
            info = node_map.get(member_id, {})
            peers.append(
                {
                    "hostname": info.get("hostname"),
                    "node_id": member_id,
                    "pod_id": info.get("pod_id"),
                    "role": info.get("role"),
                }
            )

        if peers:
            return peers, group
        # メンバーが自ノードのみのグループは vPC 未構成として扱う
        return [], None

    return [], None


# ==========================================================================
# PostgreSQL: 収容機器（rm_hostname）ベースの冗長 Leaf 特定
# ==========================================================================
REDUNDANT_LEAF_QUERY = """
WITH recent AS (
    -- 直近 N 日ぶんだけを対象にする。以降の CTE はすべてここを参照し、
    -- t_if の全履歴を走査しないようにする
    SELECT *
    FROM t_if
    WHERE time >= CURRENT_DATE - INTERVAL '{days} days'
),
rm_hosts AS (
    -- Step 1: 指定 hostname に紐づく rm_hostname を if_descr の分割で抽出（空白は無視）
    SELECT DISTINCT SPLIT_PART(if_descr, ',', 2) AS rm_hostname
    FROM recent
    WHERE hostname = %(hostname)s
      AND if_usage = 'epg'
      AND SPLIT_PART(if_descr, ',', 2) <> ''
      AND SPLIT_PART(if_descr, ',', 2) <> 'x'
      AND SPLIT_PART(if_descr, ',', 2) !~ 'SpSw|Leaf'
),
hosts_in_same_rm AS (
    -- Step 2: 同じ rm_hostname を共有する hostname を抽出（空白は無視）
    SELECT DISTINCT hostname
    FROM recent
    WHERE SPLIT_PART(if_descr, ',', 2) IN (SELECT rm_hostname FROM rm_hosts)
      AND SPLIT_PART(if_descr, ',', 2) <> ''
      AND SPLIT_PART(if_descr, ',', 2) <> 'x'
      AND SPLIT_PART(if_descr, ',', 2) !~ 'SpSw|Leaf'
),
selected_area_network AS (
    SELECT DISTINCT area_network
    FROM recent
    WHERE hostname = %(hostname)s
),
selected_rm_hostname AS (
    SELECT DISTINCT SPLIT_PART(if_descr, ',', 2) AS rm_hostname
    FROM recent
    WHERE hostname = %(hostname)s
)
SELECT *
FROM (
    SELECT time, area_network, area, network, station, hostname, nodeid, if_id, sfp_model,
           sfp_serial, if_usage, oper_status, if_descr,
           SPLIT_PART(if_descr, ',', 1) AS system,
           SPLIT_PART(if_descr, ',', 2) AS rm_hostname,
           SPLIT_PART(if_descr, ',', 3) AS rm_if,
           SPLIT_PART(if_descr, ',', 4) AS l2_node,
           SPLIT_PART(if_descr, ',', 5) AS number,
           SPLIT_PART(if_descr, ',', 6) AS comment,
           SPLIT_PART(if_id, '/', 1) AS if_id_part1,
           CAST(SPLIT_PART(if_id, '/', 2) AS INTEGER) AS if_id_part2,
           MAX(time) OVER (PARTITION BY hostname, if_id) AS max_time
    FROM recent
    -- hostname は PARTITION BY のキーなので、内側で絞っても max_time は変わらない
    WHERE hostname IN (SELECT hostname FROM hosts_in_same_rm)
) AS subquery
WHERE time = max_time
  AND area_network IN (SELECT area_network FROM selected_area_network)
  AND rm_hostname IN (SELECT rm_hostname FROM selected_rm_hostname)
ORDER BY area, hostname, if_id_part1, if_id_part2;
"""


def jsonable(value):
    """datetime 等を JSON 出力可能な型へ変換する。"""
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def is_leaf_hostname(hostname):
    """命名規約に基づき Leaf かどうかを判定する。"""
    return bool(hostname) and ("Leaf" in hostname or "leaf" in hostname)


def fetch_redundant_rows(hostname):
    """冗長 Leaf 特定クエリを実行し、全行を dict のリストで返す。"""
    return fetch_dict_rows_from_psql(
        psql_host,
        psql_db,
        psql_user,
        psql_password,
        REDUNDANT_LEAF_QUERY.format(days=psql_lookback_days),
        {"hostname": hostname},
    )


def summarize_redundant_rows(hostname, rows):
    """
    クエリ結果から、対象ホスト以外の Leaf を「共有している収容機器」ごとに集計する。

    Returns:
        (peers, skipped)
        peers   : [{"hostname","node_id","area_network","shared_rm_hostnames",
                    "shared_port_count","if_ids"}, ...]  共有ポート数の降順
        skipped : Leaf 命名規約に一致せず除外したホスト名のリスト
    """
    summary: Dict[str, Dict[str, Any]] = {}
    skipped = set()

    for row in rows:
        peer_hostname = row.get("hostname")
        if not peer_hostname or peer_hostname == hostname:
            continue
        if not is_leaf_hostname(peer_hostname):
            skipped.add(peer_hostname)
            continue

        entry = summary.setdefault(
            peer_hostname,
            {
                "hostname": peer_hostname,
                "node_id": str(row.get("nodeid")) if row.get("nodeid") else None,
                "area_network": row.get("area_network"),
                "shared_rm_hostnames": set(),
                "if_ids": [],
            },
        )
        rm_hostname = row.get("rm_hostname")
        if rm_hostname:
            entry["shared_rm_hostnames"].add(rm_hostname)
        if_id = row.get("if_id")
        if if_id and if_id not in entry["if_ids"]:
            entry["if_ids"].append(if_id)

    peers = []
    for entry in summary.values():
        peers.append(
            {
                "hostname": entry["hostname"],
                "node_id": entry["node_id"],
                "area_network": entry["area_network"],
                "shared_rm_hostnames": sorted(entry["shared_rm_hostnames"]),
                "shared_port_count": len(entry["if_ids"]),
                "if_ids": entry["if_ids"],
            }
        )

    peers.sort(key=lambda p: (-p["shared_port_count"], p["hostname"]))
    return peers, sorted(skipped)


def write_psql_detail(log_directory, pid, hostname, rows):
    """PSQL クエリの生結果をホスト単位の JSON ファイルへ保存する。"""
    detail_path = f"{log_directory}/{pid}_{hostname}_psql_detail.json"
    serializable = [{k: jsonable(v) for k, v in row.items()} for row in rows]
    with open(detail_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, ensure_ascii=False, indent=4)
    return detail_path


# ==========================================================================
# 引数
# ==========================================================================
def comma_separated_list(value):
    return [x.strip() for x in re.split(r"[,\s]+", value) if x.strip()]


# ==========================================================================
# 正常性確認ツール連携
# ==========================================================================
# 別ツール（normalcy_check/main.py）を冗長Leafに対して実行し、
# 最後に出力される "Total : OK/NG" を判定に使う。
# 出力そのものは <pid>_<hostname>_normalcy.log に保存する。

TOTAL_LINE_PATTERN = re.compile(r"^\s*Total\s*[:：]\s*(OK|NG)\s*$", re.IGNORECASE)


def parse_normalcy_total(output):
    """
    正常性確認ツールの出力から Total 行の判定（OK/NG）を取り出す。

    Returns:
        "OK" / "NG" / None（Total 行が見つからない＝判定不能）
    """
    verdict = None
    for line in output.splitlines():
        m = TOTAL_LINE_PATTERN.match(line)
        if m:
            # 複数ホストぶん出力された場合は最後の Total を採用する
            verdict = m.group(1).upper()
    return verdict


def run_normalcy_check(log_directory, pid, target_node, leaf_hostname):
    """
    冗長Leaf 1台に対して正常性確認ツールを実行する。

    Returns:
        (verdict, log_path)
        verdict は "OK" / "NG" / None（判定不能）
    """
    log_path = f"{log_directory}/{pid}_{leaf_hostname}_normalcy.log"
    cmd = ["python3", normalcy_check_path, leaf_hostname]

    try:
        completed = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=normalcy_check_timeout,
        )
        output = completed.stdout.decode("utf-8", errors="replace")
        returncode = completed.returncode
    except subprocess.TimeoutExpired as e:
        partial = e.stdout.decode("utf-8", errors="replace") if e.stdout else ""
        output = partial + f"\n[TIMEOUT] {normalcy_check_timeout}秒で応答なし\n"
        returncode = None
    except Exception as e:
        output = f"[ERROR] 正常性確認ツールの実行に失敗しました: {type(e).__name__}: {e}\n"
        returncode = None

    header = (
        f"# command : {' '.join(cmd)}\n"
        f"# target  : {target_node}\n"
        f"# leaf    : {leaf_hostname}\n"
        f"# time    : {timestamp()}\n"
        f"# rc      : {returncode}\n"
        f"{'-' * 70}\n"
    )
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(header + output)
    except Exception as e:
        log_detail(
            log_directory,
            pid,
            f"{leaf_hostname}: 正常性確認ログの保存失敗: {type(e).__name__}: {e}",
        )

    verdict = parse_normalcy_total(output)
    return verdict, log_path


def get_normalcy_result(log_directory, pid, target_node, leaf_hostname):
    """
    冗長Leafの正常性確認結果を返す。同じ Leaf に対しては 1 回だけ実行し、
    2 回目以降はキャッシュを返す（複数の対象 Leaf が同じ冗長 Leaf を
    参照した場合の二重実行とログ競合を防ぐ）。

    Returns:
        (verdict, log_path, cached)
    """
    with normalcy_cache_lock:
        if leaf_hostname in normalcy_cache:
            verdict, log_path = normalcy_cache[leaf_hostname]
            return verdict, log_path, True
        host_lock = normalcy_host_locks.setdefault(leaf_hostname, threading.Lock())

    # 同じ Leaf を狙った別スレッドは、ここで先行スレッドの完了を待つ
    with host_lock:
        with normalcy_cache_lock:
            if leaf_hostname in normalcy_cache:
                verdict, log_path = normalcy_cache[leaf_hostname]
                return verdict, log_path, True

        verdict, log_path = run_normalcy_check(
            log_directory, pid, target_node, leaf_hostname
        )

        with normalcy_cache_lock:
            normalcy_cache[leaf_hostname] = (verdict, log_path)

    return verdict, log_path, False


def check_redundant_leafs(log_directory, pid, target_node, redundant_leafs):
    """
    冗長Leafを1台ずつ正常性確認ツールにかけ、結果を各要素に書き戻す。

    Returns:
        (all_ok, ng_hosts, unknown_hosts)
        all_ok は全台 OK のとき True（判定不能があれば安全側で False）
    """
    ng_hosts = []
    unknown_hosts = []

    for leaf in redundant_leafs:
        leaf_hostname = leaf.get("hostname")
        if not leaf_hostname:
            unknown_hosts.append("(hostname不明)")
            leaf["normalcy_result"] = "UNKNOWN"
            continue

        log_processing(
            log_directory, pid, f"{target_node}: {leaf_hostname} の正常性確認 開始"
        )
        verdict, log_path, cached = get_normalcy_result(
            log_directory, pid, target_node, leaf_hostname
        )

        leaf["normalcy_result"] = verdict or "UNKNOWN"
        leaf["normalcy_log_file"] = os.path.basename(log_path)
        leaf["normalcy_cached"] = cached

        suffix = "（実行済みの結果を再利用）" if cached else ""
        if verdict == "OK":
            log_processing(
                log_directory,
                pid,
                f"{target_node}: {leaf_hostname} 正常性確認 OK{suffix}",
            )
        elif verdict == "NG":
            ng_hosts.append(leaf_hostname)
            log_processing(
                log_directory,
                pid,
                f"{target_node}: {leaf_hostname} 正常性確認 NG{suffix}",
            )
        else:
            unknown_hosts.append(leaf_hostname)
            log_processing(
                log_directory,
                pid,
                f"{target_node}: {leaf_hostname} 正常性確認 判定不能（Total行なし）{suffix}",
            )

        log_detail(
            log_directory,
            pid,
            f"[NORMALCY] {target_node} -> {leaf_hostname}: {leaf['normalcy_result']} "
            f"({os.path.basename(log_path)})",
        )

    all_ok = not ng_hosts and not unknown_hosts
    return all_ok, ng_hosts, unknown_hosts


def finish_host_result(log_directory, pid, hostname, result):
    """
    冗長Leaf特定が済んだノードについて、冗長Leafの正常性確認まで行い、
    結果に応じて status.json を更新する。
    """
    redundant_leafs = result.get("redundant_leafs") or []

    if not normalcy_check_enabled:
        log_processing(
            log_directory, pid, f"{hostname}: 正常性確認は無効(スキップ)"
        )
        update_node_status(
            log_directory,
            pid,
            hostname,
            result_code.EACH_STATUS_CODE_COMPLETED,
            f"{hostname}: 冗長Leaf特定完了({result.get('redundancy_type')})",
        )
        return result

    all_ok, ng_hosts, unknown_hosts = check_redundant_leafs(
        log_directory, pid, hostname, redundant_leafs
    )

    if all_ok:
        result["normalcy_result"] = "OK"
        update_node_status(
            log_directory,
            pid,
            hostname,
            result_code.EACH_STATUS_CODE_COMPLETED,
            f"{hostname}: 冗長Leaf特定・正常性確認完了({result.get('redundancy_type')})",
        )
        return result

    reasons = []
    if ng_hosts:
        reasons.append(f"NG: {', '.join(ng_hosts)}")
    if unknown_hosts:
        reasons.append(f"判定不能: {', '.join(unknown_hosts)}")

    result["normalcy_result"] = "NG" if ng_hosts else "UNKNOWN"
    result["message"] = f"{result.get('message')} / 冗長Leaf正常性確認NG"
    update_node_status(
        log_directory,
        pid,
        hostname,
        result_code.EACH_STATUS_CODE_SERVER_ERROR,
        f"{hostname}: 冗長Leaf正常性確認NG（{' / '.join(reasons)}）",
    )
    return result


# ==========================================================================
# ホスト単位の処理
# ==========================================================================
def process_host(hostname, log_directory, pid, apic_cache, vpc_cache):
    """1ホスト分の冗長Leaf特定を行い、結果 dict を返す（スレッドから呼ばれる）。"""
    results = []
    for _ in (0,):  # continue による早期終了のため1回だけ回すループ
        try:
            log_processing(log_directory, pid, f"{hostname}: 処理開始")

            area_network = get_area_network(hostname)
            if not area_network:
                msg = f"{hostname}: area_network取得失敗"
                log_processing(log_directory, pid, msg)
                update_node_status(
                    log_directory,
                    pid,
                    hostname,
                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                    msg,
                )
                results.append(
                    {
                        "target_node": hostname,
                        "area_network": None,
                        "node_id": None,
                        "pod_id": None,
                        "redundancy_type": None,
                        "redundant_leafs": [],
                        "message": "area_network取得失敗",
                    }
                )
                continue

            # ---- APIC 接続（area_network 単位でキャッシュ）----
            with cache_lock:
                cached_apic = apic_cache.get(area_network)
            if cached_apic:
                token, apic_ip, apic = cached_apic
            else:
                try:
                    log_processing(
                        log_directory, pid, f"APIC接続試行: token_node={hostname}"
                    )
                    token, apic_ip, apic = get_token_from_random_node(hostname)
                    with cache_lock:
                        apic_cache[area_network] = (token, apic_ip, apic)
                    log_processing(
                        log_directory,
                        pid,
                        f"APIC接続成功: apic_ip={apic_ip}, apic={apic}",
                    )
                except Exception as e:
                    log_processing(log_directory, pid, f"{hostname}: APIC接続失敗")
                    log_detail(
                        log_directory,
                        pid,
                        f"APIC接続例外: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                    )
                    update_node_status(
                        log_directory,
                        pid,
                        hostname,
                        result_code.EACH_STATUS_CODE_SERVER_ERROR,
                        f"{hostname}: APIC接続失敗",
                    )
                    results.append(
                        {
                            "target_node": hostname,
                            "area_network": area_network,
                            "node_id": None,
                            "pod_id": None,
                            "redundancy_type": None,
                            "redundant_leafs": [],
                            "message": "APIC接続失敗",
                        }
                    )
                    continue

            # ---- ノードID / PodID の取得 ----
            node_id, pod_id = get_hostname_info(hostname, apic_ip, apic, token)
            if not node_id or not pod_id:
                msg = f"{hostname}: ノードID／PodIDの取得失敗"
                log_processing(log_directory, pid, msg)
                update_node_status(
                    log_directory,
                    pid,
                    hostname,
                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                    msg,
                )
                results.append(
                    {
                        "target_node": hostname,
                        "area_network": area_network,
                        "node_id": None,
                        "pod_id": None,
                        "redundancy_type": None,
                        "redundant_leafs": [],
                        "message": "ノードID／PodIDの取得失敗",
                    }
                )
                continue

            log_detail(
                log_directory,
                pid,
                f"{hostname}: node_id={node_id}, pod_id={pod_id}, area_network={area_network}",
            )

            # ---- vPC 保護グループ／topSystem マップの取得（APIC 単位でキャッシュ）----
            with cache_lock:
                cached_vpc = vpc_cache.get(apic_ip)
            if cached_vpc:
                vpc_groups, node_map = cached_vpc
            else:
                try:
                    vpc_groups = get_vpc_groups(token, apic_ip)
                    node_map = get_topsystem_map(token, apic_ip)
                    with cache_lock:
                        vpc_cache[apic_ip] = (vpc_groups, node_map)
                    log_detail(
                        log_directory,
                        pid,
                        f"vPC保護グループ取得: apic_ip={apic_ip}, groups={len(vpc_groups)}, nodes={len(node_map)}",
                    )
                except Exception as e:
                    log_processing(
                        log_directory, pid, f"{hostname}: vPC保護グループの取得失敗"
                    )
                    log_detail(
                        log_directory,
                        pid,
                        f"vPC保護グループ取得例外: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                    )
                    update_node_status(
                        log_directory,
                        pid,
                        hostname,
                        result_code.EACH_STATUS_CODE_SERVER_ERROR,
                        f"{hostname}: vPC保護グループの取得失敗",
                    )
                    results.append(
                        {
                            "target_node": hostname,
                            "area_network": area_network,
                            "node_id": node_id,
                            "pod_id": pod_id,
                            "redundancy_type": None,
                            "redundant_leafs": [],
                            "message": "vPC保護グループの取得失敗",
                        }
                    )
                    continue

            # ---- vPC ピアの判定 ----
            peers, group = find_vpc_peers(node_id, vpc_groups, node_map)

            if peers:
                redundant_leafs = [
                    {
                        "hostname": p["hostname"],
                        "node_id": p["node_id"],
                        "pod_id": p["pod_id"],
                    }
                    for p in peers
                ]
                log_processing(
                    log_directory,
                    pid,
                    f"{hostname}: vPC構成あり -> 冗長Leaf={', '.join([p['hostname'] or p['node_id'] for p in peers])}",
                )
                log_detail(
                    log_directory,
                    pid,
                    f"{hostname}: vPC保護グループ dn={group.get('dn')}, members={group.get('members')}",
                )
                result = {
                    "target_node": hostname,
                    "area_network": area_network,
                    "node_id": node_id,
                    "pod_id": pod_id,
                    "redundancy_type": "vpc",
                    "vpc_group": {
                        "name": group.get("name"),
                        "id": group.get("id"),
                        "dn": group.get("dn"),
                        "members": group.get("members"),
                    },
                    "redundant_leafs": redundant_leafs,
                    "message": "vPCピアから特定",
                }
                results.append(
                    finish_host_result(log_directory, pid, hostname, result)
                )
                continue

            # ---- vPC 未構成 -> PSQL ロジックで特定 ----
            log_processing(
                log_directory, pid, f"{hostname}: vPC未構成 -> PSQLロジックで特定"
            )

            try:
                rows = fetch_redundant_rows(hostname)
            except Exception as e:
                log_processing(log_directory, pid, f"{hostname}: PSQL検索失敗")
                log_detail(
                    log_directory,
                    pid,
                    f"PSQL検索例外: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                )
                update_node_status(
                    log_directory,
                    pid,
                    hostname,
                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                    f"{hostname}: PSQL検索失敗",
                )
                results.append(
                    {
                        "target_node": hostname,
                        "area_network": area_network,
                        "node_id": node_id,
                        "pod_id": pod_id,
                        "redundancy_type": "psql",
                        "redundant_leafs": [],
                        "message": "PSQL検索失敗",
                    }
                )
                continue

            detail_path = write_psql_detail(log_directory, pid, hostname, rows)
            log_detail(
                log_directory,
                pid,
                f"{hostname}: PSQL検索結果 {len(rows)}行 -> {detail_path}",
            )

            peers_psql, skipped = summarize_redundant_rows(hostname, rows)

            if skipped:
                log_detail(
                    log_directory,
                    pid,
                    f"{hostname}: Leaf命名規則に一致しないため除外: {', '.join(skipped)}",
                )

            # PSQL 側から node_id を補完できない場合は APIC のマップで補う
            for p in peers_psql:
                if not p.get("node_id"):
                    for nid, info in node_map.items():
                        if info.get("hostname") == p["hostname"]:
                            p["node_id"] = nid
                            break

            if not peers_psql:
                msg = f"{hostname}: 冗長Leafが見つかりませんでした"
                log_processing(log_directory, pid, msg)
                update_node_status(
                    log_directory,
                    pid,
                    hostname,
                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                    msg,
                )
                results.append(
                    {
                        "target_node": hostname,
                        "area_network": area_network,
                        "node_id": node_id,
                        "pod_id": pod_id,
                        "redundancy_type": "psql",
                        "redundant_leafs": [],
                        "psql_detail_file": os.path.basename(detail_path),
                        "message": "冗長Leafが見つかりませんでした",
                    }
                )
                continue

            log_processing(
                log_directory,
                pid,
                f"{hostname}: 冗長Leaf={', '.join([p['hostname'] for p in peers_psql])}",
            )
            result = {
                "target_node": hostname,
                "area_network": area_network,
                "node_id": node_id,
                "pod_id": pod_id,
                "redundancy_type": "psql",
                "redundant_leafs": peers_psql,
                "psql_detail_file": os.path.basename(detail_path),
                "message": "収容機器(rm_hostname)の共有から特定",
            }
            results.append(finish_host_result(log_directory, pid, hostname, result))

        except Exception as e:
            log_processing(log_directory, pid, f"{hostname}: 想定外エラー")
            log_detail(
                log_directory,
                pid,
                f"想定外エラー: {type(e).__name__}: {e}\n{traceback.format_exc()}",
            )
            update_node_status(
                log_directory,
                pid,
                hostname,
                result_code.EACH_STATUS_CODE_SERVER_ERROR,
                f"{hostname}: 想定外エラー ({type(e).__name__})",
            )
            results.append(
                {
                    "target_node": hostname,
                    "area_network": None,
                    "node_id": None,
                    "pod_id": None,
                    "redundancy_type": None,
                    "redundant_leafs": [],
                    "message": f"想定外エラー ({type(e).__name__})",
                }
            )
    return results[0] if results else None

# ==========================================================================
# main
# ==========================================================================
def main():
    # ==== 入口処理: 引数を受け取り、最低限のバリデーションを実施 ====
    parser = argparse.ArgumentParser(description="redundant_leaf tool")
    parser.add_argument("--target_nodes", type=comma_separated_list)
    parser.add_argument("--pid", help="PID")
    parser.add_argument("--order_group", help="order group")
    args = parser.parse_args()

    # 同じホストが複数指定されても1回だけ扱う（status.json の重複防止）
    hostnames = list(dict.fromkeys(args.target_nodes or []))
    uid = args.order_group
    pid = args.pid

    log_directory = f"{script_directory}/log/{uid}"

    status_path = f"{log_directory}/{pid}_status.json"
    processing_log = f"{log_directory}/{pid}_processing.log"
    detail_log = f"{log_directory}/{pid}_detail.log"
    output_path = f"{log_directory}/{pid}_redundant_leaf.json"

    errors = []

    if not hostnames:
        errors.append("target_nodes が指定されていません。")

    if not uid:
        errors.append("order_group が指定されていません。")

    if not pid:
        errors.append("PID が指定されていません。")

    if hostnames and not all(is_leaf_hostname(h) for h in hostnames):
        errors.append("target_nodes に Leaf の命名規則と一致しないホストが含まれています。")

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

    os.makedirs(log_directory, exist_ok=True)

    if any(
        os.path.exists(p)
        for p in [status_path, processing_log, detail_log, output_path]
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

    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(json_data_structure, f, ensure_ascii=False, indent=4)

    for name in ["processing", "detail"]:
        open(f"{log_directory}/{pid}_{name}.log", "w").close()

    log_processing(
        log_directory,
        pid,
        f"ログ初期化: {log_directory}/{pid}_processing.log, {log_directory}/{pid}_detail.log",
    )

    # ==== 許可リスト（t_ch）での hostname 存在確認 ====
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

    # ==== 事前確認: APIC への接続 ====
    log_processing(log_directory, pid, "冗長Leaf特定 開始")

    try:
        token_node = random.choice(hostnames)
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
        fail_all_and_exit(log_directory, pid, hostnames, "APIC接続失敗")

    # ==== 冗長 Leaf の特定 ====

    # APIC 接続情報は area_network 単位でキャッシュする
    apic_cache: Dict[str, Tuple[str, str, str]] = {}
    # vPC 保護グループ／ノードマップは APIC 単位でキャッシュする
    vpc_cache: Dict[str, Tuple[List[Dict[str, Any]], Dict[str, Dict[str, str]]]] = {}

    unique_hosts = hostnames

    result_map: Dict[str, Any] = {}
    workers = max(1, min(int(max_workers), len(unique_hosts)))
    log_processing(
        log_directory,
        pid,
        f"冗長Leaf特定を開始: 対象{len(unique_hosts)}台, 並列度={workers}",
    )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(
                process_host, h, log_directory, pid, apic_cache, vpc_cache
            ): h
            for h in unique_hosts
        }
        for future in as_completed(future_map):
            h = future_map[future]
            try:
                result_map[h] = future.result()
            except Exception as e:
                log_processing(log_directory, pid, f"{h}: 想定外エラー")
                log_detail(
                    log_directory,
                    pid,
                    f"想定外エラー: {type(e).__name__}: {e}\n{traceback.format_exc()}",
                )
                update_node_status(
                    log_directory,
                    pid,
                    h,
                    result_code.EACH_STATUS_CODE_SERVER_ERROR,
                    f"{h}: 想定外エラー ({type(e).__name__})",
                )
                result_map[h] = {
                    "target_node": h,
                    "area_network": None,
                    "node_id": None,
                    "pod_id": None,
                    "redundancy_type": None,
                    "redundant_leafs": [],
                    "message": f"想定外エラー ({type(e).__name__})",
                }

    # 出力順は --target_nodes の指定順に揃える
    results = [result_map[h] for h in unique_hosts if result_map.get(h)]


    # ==== JSON 出力 ====

    output_data = {
        "order_group": uid,
        "pid": pid,
        "timestamp": timestamp(),
        "results": results,
    }

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)
        log_processing(log_directory, pid, f"冗長Leaf情報を出力: {output_path}")
    except Exception as e:
        log_processing(log_directory, pid, "冗長Leaf情報の出力失敗")
        log_detail(
            log_directory,
            pid,
            f"JSON出力例外: {type(e).__name__}: {e}\n{traceback.format_exc()}",
        )
        fail_all_and_exit(log_directory, pid, hostnames, "冗長Leaf情報の出力失敗")

    finalize_status(log_directory, pid)
    log_processing(log_directory, pid, "冗長Leaf特定 終了")


if __name__ == "__main__":
    main()