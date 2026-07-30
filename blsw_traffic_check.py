#!/usr/bin/env python3
"""
blsw_traffic_check.py

Grafana(Zabbixデータソース)のダッシュボードから、BorderLeaf-SpSw間の
総トラフィック現在値が基準値以上かを確認するモジュール。

nodeshut_vup.py から import して使う:
    import blsw_traffic_check
    ok = blsw_traffic_check.check_area(area_network)  # True=OK / False=NG
    # 判定不能(接続失敗・ダッシュボード0件など)は例外 GrafanaCheckError

設定は credentials.py から読む:
    GRAFANA_CHECK_ENABLED
    GRAFANA_URL / GRAFANA_USER / GRAFANA_PASSWORD
    GRAFANA_PANEL_IN_CURRENT / GRAFANA_PANEL_IN_BASELINE
    GRAFANA_PANEL_OUT_CURRENT / GRAFANA_PANEL_OUT_BASELINE

単体実行も可能(動作確認用):
    python3 blsw_traffic_check.py B-OYM
"""

import re
import sys
from datetime import datetime, timedelta, timezone

import requests
import urllib3

import credentials

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GrafanaCheckError(Exception):
    """Grafana確認が判定不能(接続失敗・ダッシュボード未検出・データ無しなど)。"""
    pass


# ----------------------------------------------------------------------
# 設定読み込み(credentials から。未定義でも安全なデフォルト)
# ----------------------------------------------------------------------
def _cfg(name, default=None):
    return getattr(credentials, name, default)


GRAFANA_CHECK_ENABLED = _cfg("GRAFANA_CHECK_ENABLED", False)
GRAFANA_URL = _cfg("GRAFANA_URL", "")
GRAFANA_USER = _cfg("GRAFANA_USER", "")
GRAFANA_PASSWORD = _cfg("GRAFANA_PASSWORD", "")

PANEL_IN_CURRENT = _cfg("GRAFANA_PANEL_IN_CURRENT", "各BorderLeaf(In)-各SpSw間の総トラヒック最新値")
PANEL_IN_BASELINE = _cfg("GRAFANA_PANEL_IN_BASELINE", "正常性確認時の基準値(In)")
PANEL_OUT_CURRENT = _cfg("GRAFANA_PANEL_OUT_CURRENT", "各BorderLeaf(Out)-各SpSw間の総トラヒック最新値")
PANEL_OUT_BASELINE = _cfg("GRAFANA_PANEL_OUT_BASELINE", "正常性確認時の基準値(Out)")

# 低トラフィック救済しきい値（Gb/s 表示値と同じ土俵）。
# IN/OUT のどちらか一方でも現在値がこの値以下なら、
# 基準値パネルとの比較が NG でも OK に救済する。
# None または未設定なら救済しない（従来どおりの判定）。
LOW_TRAFFIC_OK_GB = _cfg("GRAFANA_LOW_TRAFFIC_OK_GB", None)


def _new_session():
    s = requests.Session()
    s.auth = (GRAFANA_USER, GRAFANA_PASSWORD)
    s.verify = False
    return s


# ----------------------------------------------------------------------
# 内部ヘルパー(元スクリプトのロジックを踏襲)
# ----------------------------------------------------------------------
def parse_duration(s):
    """'7d', '24h', '5m' -> timedelta"""
    m = re.match(r"(?:now-)?(\d+)([dhm])$", s.strip())
    if not m:
        raise GrafanaCheckError(f"期間文字列を解釈できません: {s}")
    n, unit = int(m.group(1)), m.group(2)
    return {
        "d": timedelta(days=n),
        "h": timedelta(hours=n),
        "m": timedelta(minutes=n),
    }[unit]


def search_dashboard(session, query):
    """検索文言からダッシュボードを1件解決する。複数/0件はエラー。"""
    try:
        resp = session.get(
            f"{GRAFANA_URL}/api/search",
            params={"query": query},
            proxies={"http": None, "https": None},
            timeout=20,
        )
        resp.raise_for_status()
    except GrafanaCheckError:
        raise
    except Exception as e:
        raise GrafanaCheckError(f"ダッシュボード検索失敗 '{query}': {type(e).__name__}: {e}")

    results = [r for r in resp.json() if r.get("type") == "dash-db"]
    if not results:
        raise GrafanaCheckError(f"ダッシュボードが見つかりません: '{query}'")
    if len(results) > 1:
        titles = ", ".join(f"{r['title']}(uid={r['uid']})" for r in results)
        raise GrafanaCheckError(
            f"'{query}' で複数ヒット。より具体的なキーワードが必要: {titles}"
        )
    return results[0]["uid"], results[0]["title"]


def get_dashboard(session, uid):
    try:
        resp = session.get(
            f"{GRAFANA_URL}/api/dashboards/uid/{uid}",
            proxies={"http": None, "https": None},
            timeout=20,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise GrafanaCheckError(f"ダッシュボード取得失敗 uid={uid}: {type(e).__name__}: {e}")


def find_panel(dash_json, title):
    """同名パネルが複数ある場合(グラフ+数値)、stat パネルを優先する。"""
    panels = dash_json["dashboard"]["panels"]
    matches = [p for p in panels if p.get("title") == title]
    if not matches:
        raise GrafanaCheckError(f"パネルが見つかりません: '{title}'")
    if len(matches) == 1:
        return matches[0]
    stat_matches = [p for p in matches if p.get("type") == "stat"]
    if len(stat_matches) == 1:
        return stat_matches[0]
    raise GrafanaCheckError(f"同名パネルが複数あり一意に特定できません: '{title}'")


def resolve_time_range(panel, default_from="now-5m"):
    now = datetime.now(timezone.utc)
    time_from = panel.get("timeFrom", default_from) or default_from
    time_shift = panel.get("timeShift")

    to_dt = now
    from_dt = now - parse_duration(time_from)

    if time_shift:
        shift = parse_duration(time_shift)
        to_dt -= shift
        from_dt -= shift

    fmt = "%Y-%m-%dT%H:%M:%S.000Z"
    return from_dt.strftime(fmt), to_dt.strftime(fmt)


def get_multiplier(panel):
    for t in panel.get("transformations", []):
        if t.get("id") == "calculateField":
            right = t["options"].get("binary", {}).get("right")
            if right is not None:
                return float(right)
    return 1.0


def reduce_values(values, calc):
    values = [v for v in values if v is not None]
    if not values:
        raise GrafanaCheckError("データポイントが取得できません")
    if calc in ("lastNotNull", "last"):
        return values[-1]
    if calc == "min":
        return min(values)
    if calc == "max":
        return max(values)
    if calc == "mean":
        return sum(values) / len(values)
    raise GrafanaCheckError(f"未対応の calc: {calc}")


def get_panel_value(session, panel, calc_override=None):
    ds_uid = panel["datasource"]["uid"]
    target = dict(panel["targets"][0])
    target.update({
        "refId": "A",
        "datasource": {"uid": ds_uid, "type": "alexanderzobnin-zabbix-datasource"},
    })

    time_from, time_to = resolve_time_range(panel)
    body = {"from": time_from, "to": time_to, "queries": [target]}

    try:
        resp = session.post(
            f"{GRAFANA_URL}/api/ds/query",
            json=body,
            proxies={"http": None, "https": None},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        values = data["results"]["A"]["frames"][0]["data"]["values"][1]
    except GrafanaCheckError:
        raise
    except Exception as e:
        raise GrafanaCheckError(f"パネル値取得失敗: {type(e).__name__}: {e}")

    if calc_override:
        calc = calc_override
    else:
        calc = panel.get("options", {}).get("reduceOptions", {}).get(
            "calcs", ["lastNotNull"]
        )[0]

    raw = reduce_values(values, calc)
    return raw * get_multiplier(panel)


def _check_pair(session, dash_json, current_title, baseline_title, label, log=None):
    """現在値 >= 基準値 か判定。(ok:bool, current_gb:float, message:str) を返す。"""
    current = get_panel_value(
        session, find_panel(dash_json, current_title), calc_override="lastNotNull"
    )
    threshold = get_panel_value(
        session, find_panel(dash_json, baseline_title), calc_override="min"
    )
    current_gb = current / 1_000_000_000
    threshold_gb = threshold / 1_000_000_000
    ok = current >= threshold
    msg = (
        f"{label}: 現在値 {current_gb:.3f} Gb/s "
        f"{'>=' if ok else '<'} 基準値 {threshold_gb:.3f} Gb/s "
        f"[{'OK' if ok else 'NG'}]"
    )
    if log:
        log(msg)
    else:
        print(msg)
    return ok, current_gb, msg


# ----------------------------------------------------------------------
# 公開API
# ----------------------------------------------------------------------
def is_enabled():
    """Grafana確認が有効か。"""
    return bool(GRAFANA_CHECK_ENABLED)


def check_area(area_network, log=None):
    """
    指定 area_network(=ダッシュボード検索文言)について、
    IN/OUT の現在値が基準値以上かを確認する。

    返り値:
        True  = OK(IN/OUT 両方が基準値以上)
                または 低トラフィック救済に該当した場合
        False = NG(どちらかが基準値未満、かつ救済にも該当しない)

    低トラフィック救済:
        GRAFANA_LOW_TRAFFIC_OK_GB が設定されている場合、
        IN/OUT のどちらか一方でも現在値がその値(Gb/s)以下なら、
        基準値パネル比較が NG でも OK として通す。

    例外:
        GrafanaCheckError = 判定不能(接続失敗/ダッシュボード未検出/データ無し 等)
        呼び出し側で、判定不能を OK扱い/NG扱い/スキップ のどれにするか決める。

    log: 任意のログ関数(message:str を受ける)。None なら print。
    """
    if not area_network:
        raise GrafanaCheckError("area_network が空です")

    session = _new_session()

    uid, title = search_dashboard(session, area_network)
    if log:
        log(f"Grafana対象ダッシュボード: {title} (uid={uid}) query='{area_network}'")
    else:
        print(f"対象ダッシュボード: {title} (uid={uid})")

    dash_json = get_dashboard(session, uid)

    in_ok, in_gb, _ = _check_pair(
        session, dash_json, PANEL_IN_CURRENT, PANEL_IN_BASELINE, "IN", log
    )
    out_ok, out_gb, _ = _check_pair(
        session, dash_json, PANEL_OUT_CURRENT, PANEL_OUT_BASELINE, "OUT", log
    )

    result = in_ok and out_ok

    # 低トラフィック救済: IN/OUT のどちらか一方でも現在値がしきい値以下なら、
    # 基準値パネル比較が NG でも OK として通す。
    if not result and LOW_TRAFFIC_OK_GB is not None:
        try:
            threshold_gb = float(LOW_TRAFFIC_OK_GB)
        except (TypeError, ValueError):
            threshold_gb = None
        if threshold_gb is not None and (
            in_gb <= threshold_gb or out_gb <= threshold_gb
        ):
            result = True
            msg = (
                f"低トラフィック救済: IN {in_gb:.3f} / OUT {out_gb:.3f} Gb/s の"
                f"いずれかが基準 {threshold_gb:.3f} Gb/s 以下のため OK 扱い"
            )
            if log:
                log(msg)
            else:
                print(msg)

    return result


# ----------------------------------------------------------------------
# 単体実行(動作確認用)
# ----------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Grafana Zabbixパネルの閾値チェック(単体実行)")
    parser.add_argument("area_network", help="ダッシュボード検索文言(=area_network、例: B-OYM)")
    args = parser.parse_args()

    if not is_enabled():
        print("[INFO] GRAFANA_CHECK_ENABLED=False のため確認は無効化されています(単体実行では続行)。")

    try:
        ok = check_area(args.area_network)
    except GrafanaCheckError as e:
        print(f"[ERROR] 判定不能: {e}", file=sys.stderr)
        sys.exit(2)

    print(f"総合判定: {'OK' if ok else 'NG'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()