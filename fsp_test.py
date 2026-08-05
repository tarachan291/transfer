import argparse
import getpass
import json
import re
import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Forwarding Scale Profile の確認・適用を行う検証用スクリプト。
# comm_decomm.py には組み込まず、単体で挙動を確認するために使う。
#
# APIC / 認証情報 / ノードID はすべて引数で指定する。
# DB も設定ファイルも参照しないため、requests だけで動作する。
#
# 適用の構造:
#   topoctrlFwdScaleProfilePol（プロファイル定義）
#       ↑ infraRsTopoctrlFwdScaleProfPol
#   infraAccNodePGrp（Node Policy Group）
#       ↑ infraRsAccNodePGrp
#   infraLeafS（セレクタ）＋ infraNodeBlk（ノードID指定）
#   infraNodeP（Leaf Profile）
#
# ノード個別に変更できるかどうかは「セレクタが対象ノードだけを含んでいるか」で決まる。
# 本スクリプトは show でその判定材料を出し、apply では既存 Policy Group の
# 参照先プロファイルを差し替える（パターンA）。

protocol = "https"


# ============================================================
# APIC 接続
# ============================================================


def get_session(apic_ip, username, password):
    auth = {"aaaUser": {"attributes": {"name": username, "pwd": password}}}
    s = requests.Session()
    s.verify = False
    r = s.post(
        f"{protocol}://{apic_ip}/api/aaaLogin.json",
        json=auth,
        proxies={"http": None, "https": None},
    )
    r.raise_for_status()
    token = r.json()["imdata"][0]["aaaLogin"]["attributes"]["token"]
    s.headers.update(
        {"Cookie": f"APIC-Cookie={token}", "Content-Type": "application/json"}
    )
    return s


def api_get(session, apic_ip, path):
    url = f"{protocol}://{apic_ip}{path}"
    r = session.get(url, proxies={"http": None, "https": None}, timeout=30)
    r.raise_for_status()
    return r.json().get("imdata", [])


def api_post(session, apic_ip, path, payload):
    url = f"{protocol}://{apic_ip}{path}"
    print(f"\n  POST {url}")
    print(f"  {json.dumps(payload, ensure_ascii=False)}")
    r = session.post(
        url, json=payload, proxies={"http": None, "https": None}, timeout=30
    )
    if r.status_code >= 400:
        print(f"  -> {r.status_code}: {r.text}")
        r.raise_for_status()
    print(f"  -> {r.status_code} OK")
    return r.json()


def attrs_of(item, cls):
    return item.get(cls, {}).get("attributes", {})


# ============================================================
# 情報取得
# ============================================================


def get_node_by_hostname(session, apic_ip, hostname):
    imdata = api_get(
        session,
        apic_ip,
        f'/api/node/class/fabricNode.json?query-target-filter=eq(fabricNode.name,"{hostname}")',
    )
    if not imdata:
        return None
    a = attrs_of(imdata[0], "fabricNode")
    pod = re.search(r"pod-(\d+)", a.get("dn", ""))
    return {
        "hostname": hostname,
        "node_id": a.get("id", ""),
        "pod_id": pod.group(1) if pod else "1",
        "role": a.get("role", ""),
        "model": a.get("model", ""),
        "version": a.get("version", ""),
        "fabric_st": a.get("fabricSt", ""),
    }


def get_node_by_id(session, apic_ip, node_id, pod_id="1"):
    imdata = api_get(
        session,
        apic_ip,
        f'/api/node/class/fabricNode.json?query-target-filter=eq(fabricNode.id,"{node_id}")',
    )
    if not imdata:
        return None
    a = attrs_of(imdata[0], "fabricNode")
    pod = re.search(r"pod-(\d+)", a.get("dn", ""))
    return {
        "hostname": a.get("name", ""),
        "node_id": a.get("id", node_id),
        "pod_id": pod.group(1) if pod else pod_id,
        "role": a.get("role", ""),
        "model": a.get("model", ""),
        "version": a.get("version", ""),
        "fabric_st": a.get("fabricSt", ""),
    }


def get_current_profile(session, apic_ip, node_id, pod_id):
    """topoctrlFwdScaleProf から profType（設定値）と currentProfile（稼働値）を取得。"""
    imdata = api_get(
        session,
        apic_ip,
        f"/api/node/class/topology/pod-{pod_id}/node-{node_id}/topoctrlFwdScaleProf.json",
    )
    if not imdata:
        return None
    a = attrs_of(imdata[0], "topoctrlFwdScaleProf")
    current = a.get("currentProfile", "")
    return {
        "profType": a.get("profType", ""),
        "currentProfile": current,
        # "sys/configProfile/cfgent-ipv4" -> "ipv4"
        "currentProfileName": current.split("cfgent-")[-1] if current else "",
        "raw": a,
    }


def list_profile_policies(session, apic_ip):
    """定義済みの Forwarding Scale Profile Policy 一覧。"""
    imdata = api_get(session, apic_ip, "/api/class/topoctrlFwdScaleProfilePol.json")
    return [attrs_of(i, "topoctrlFwdScaleProfilePol") for i in imdata]


def resolve_profile_name(pols, prof_type):
    """profType から topoctrlFwdScaleProfilePol の name を逆引きする。

    表記ゆれ（highDualStack / high-dual-stack）を吸収して比較する。
    候補が複数ある場合は決められないため、候補一覧を返して呼び出し側で判断させる。
    """
    matched = [p for p in pols if same_profile(p.get("profType", ""), prof_type)]
    return matched


def trace_policy_chain(session, apic_ip, node_id):
    """対象ノード → セレクタ → Policy Group → プロファイルの経路を辿る。

    戻り値は経路の候補リスト（複数の Leaf Profile に属する場合があるため）。
    """
    blocks = api_get(
        session,
        apic_ip,
        "/api/class/infraNodeBlk.json"
        f'?query-target-filter=and(le(infraNodeBlk.from_,"{node_id}"),'
        f'ge(infraNodeBlk.to_,"{node_id}"))',
    )

    chains = []
    for b in blocks:
        ba = attrs_of(b, "infraNodeBlk")
        blk_dn = ba.get("dn", "")

        # infraNodeBlk の DN から親のセレクタ DN を取り出す
        # uni/infra/nprof-X/leaves-Y-typ-range/nodeblk-Z
        selector_dn = blk_dn.rsplit("/", 1)[0]
        if "/leaves-" not in selector_dn:
            # Access Policies 以外（fabric 配下など）は対象外
            continue

        # セレクタ配下の全 NodeBlk を見て、対象ノード以外を含むか判定する
        sib = api_get(
            session,
            apic_ip,
            f"/api/node/mo/{selector_dn}.json"
            "?query-target=children&target-subtree-class=infraNodeBlk",
        )
        covered = []
        for s in sib:
            sa = attrs_of(s, "infraNodeBlk")
            covered.append((sa.get("from_", ""), sa.get("to_", "")))

        # セレクタに紐づく Node Policy Group
        pgrp_rel = api_get(
            session,
            apic_ip,
            f"/api/node/mo/{selector_dn}.json"
            "?query-target=children&target-subtree-class=infraRsAccNodePGrp",
        )
        pgrp_dn = (
            attrs_of(pgrp_rel[0], "infraRsAccNodePGrp").get("tDn", "")
            if pgrp_rel
            else ""
        )

        # Policy Group が参照しているプロファイル
        prof_name = ""
        prof_state = ""
        if pgrp_dn:
            prof_rel = api_get(
                session,
                apic_ip,
                f"/api/node/mo/{pgrp_dn}.json"
                "?query-target=children&target-subtree-class=infraRsTopoctrlFwdScaleProfPol",
            )
            if prof_rel:
                pa = attrs_of(prof_rel[0], "infraRsTopoctrlFwdScaleProfPol")
                prof_name = pa.get("tnTopoctrlFwdScaleProfilePolName", "")
                prof_state = pa.get("state", "")

        chains.append(
            {
                "node_blk_dn": blk_dn,
                "selector_dn": selector_dn,
                "leaf_profile": selector_dn.split("/nprof-")[-1].split("/")[0],
                "covered_blocks": covered,
                "policy_group_dn": pgrp_dn,
                "policy_group": pgrp_dn.split("accnodepgrp-")[-1] if pgrp_dn else "",
                "profile_policy": prof_name,
                "profile_state": prof_state,
            }
        )

    return chains


def covers_only(covered_blocks, node_id):
    """セレクタが対象ノード 1 台だけを含んでいるか。"""
    for f, t in covered_blocks:
        if not f or not t:
            return False
        if not (f == t == str(node_id)):
            return False
    return len(covered_blocks) == 1


# ============================================================
# 表示
# ============================================================


def show(session, apic_ip, node, target_profile=None):
    print("=" * 68)
    print(f"ノード: {node['hostname']} (node-{node['node_id']} / pod-{node['pod_id']})")
    print(
        f"  role={node['role']}  model={node['model']}  "
        f"version={node['version']}  fabricSt={node['fabric_st']}"
    )

    print("-" * 68)
    prof = get_current_profile(session, apic_ip, node["node_id"], node["pod_id"])
    if not prof:
        print("Forwarding Scale Profile: topoctrlFwdScaleProf を取得できません")
    else:
        print("Forwarding Scale Profile")
        print(f"  profType       (設定値): {prof['profType'] or '(なし)'}")
        print(f"  currentProfile (稼働値): {prof['currentProfileName'] or '(なし)'}")
        if (
            prof["profType"]
            and prof["currentProfileName"]
            and not same_profile(prof["profType"], prof["currentProfileName"])
        ):
            print("  ** 設定値と稼働値が異なります（リロード待ちの可能性）**")

    print("-" * 68)
    print("定義済みプロファイルポリシー (topoctrlFwdScaleProfilePol)")
    pols = list_profile_policies(session, apic_ip)
    if not pols:
        print("  (なし。Default のみで運用されている状態)")
    for p in pols:
        print(f"  - {p.get('name', ''):<32} profType={p.get('profType', '')}")

    print("-" * 68)
    print("適用経路 (infraNodeBlk → infraLeafS → infraAccNodePGrp → Policy)")
    chains = trace_policy_chain(session, apic_ip, node["node_id"])
    if not chains:
        print("  対象ノードを含む Leaf Profile が見つかりません")
        print("  -> Policy Group / Leaf Profile の新規作成が必要です")
    for c in chains:
        single = covers_only(c["covered_blocks"], node["node_id"])
        blocks = ", ".join(
            f"{f}" if f == t else f"{f}-{t}" for f, t in c["covered_blocks"]
        )
        print(f"  Leaf Profile : {c['leaf_profile']}")
        print(f"    セレクタ   : {c['selector_dn']}")
        print(f"    対象ノード : {blocks}  ({'単独' if single else '複数'})")
        print(f"    Policy Group: {c['policy_group'] or '(未紐づけ)'}")
        print(f"    Profile     : {c['profile_policy'] or '(未指定 = Default)'}")
        if c["profile_state"]:
            print(f"    rel state   : {c['profile_state']}")
        if not single:
            print("    ** このセレクタは複数ノードを含むため、変更は全ノードに及びます **")

    if target_profile:
        print("-" * 68)
        print(f"適用可否の判定 (target={target_profile})")
        judge(chains, pols, node, target_profile)

    print("=" * 68)


def same_profile(a, b):
    """profType（キャメルケース）と currentProfile 名（ハイフン区切り）を比較する。"""
    norm = lambda s: re.sub(r"[-_]", "", s).lower()
    return norm(a) == norm(b)


def judge(chains, pols, node, target_profile):
    if not any(p.get("name") == target_profile for p in pols):
        print(f"  NG: プロファイルポリシー '{target_profile}' が存在しません")
        print("      先に topoctrlFwdScaleProfilePol を作成してください")
        return False

    if not chains:
        print("  NG: 適用先の Policy Group が特定できません")
        return False

    applicable = [c for c in chains if c["policy_group_dn"]]
    if not applicable:
        print("  NG: セレクタに Policy Group が紐づいていません")
        return False

    ok = True
    for c in applicable:
        single = covers_only(c["covered_blocks"], node["node_id"])
        if c["profile_policy"] == target_profile:
            print(f"  済: {c['policy_group']} は既に {target_profile} を参照しています")
            continue
        if single:
            print(f"  OK: {c['policy_group']} の参照先を差し替えれば対象ノードのみ変更されます")
        else:
            print(
                f"  注意: {c['policy_group']} は複数ノードに適用されています。"
                "vPC ペアなら問題ありませんが、それ以外は影響範囲を確認してください"
            )
            ok = False
    return ok


# ============================================================
# 適用
# ============================================================


def apply_profile(session, apic_ip, node, target_profile, dry_run=False, force=False):
    chains = trace_policy_chain(session, apic_ip, node["node_id"])
    pols = list_profile_policies(session, apic_ip)

    if not judge(chains, pols, node, target_profile) and not force:
        print("\n中止しました（--force で強行できます）")
        return False

    targets = [
        c
        for c in chains
        if c["policy_group_dn"] and c["profile_policy"] != target_profile
    ]
    if not targets:
        print("\n変更の必要はありません")
        return True

    for c in targets:
        payload = {
            "infraRsTopoctrlFwdScaleProfPol": {
                "attributes": {
                    "dn": f"{c['policy_group_dn']}/rstopoctrlFwdScaleProfPol",
                    "tnTopoctrlFwdScaleProfilePolName": target_profile,
                    "status": "created,modified",
                }
            }
        }
        path = f"/api/node/mo/{c['policy_group_dn']}.json"

        if dry_run:
            print(f"\n  [DRY RUN] POST {protocol}://{apic_ip}{path}")
            print(f"  [DRY RUN] {json.dumps(payload, ensure_ascii=False)}")
        else:
            api_post(session, apic_ip, path, payload)

    if not dry_run:
        print("\n投入後の状態:")
        prof = get_current_profile(session, apic_ip, node["node_id"], node["pod_id"])
        if prof:
            print(f"  profType       (設定値): {prof['profType']}")
            print(f"  currentProfile (稼働値): {prof['currentProfileName']}")
        print("\n反映には対象ノードのリロードが必要です（--action reload）")
        print("vPC ペアの場合、リロードはトラフィック断を避けるため 1 台ずつ実施してください")

    return True


def reload_node(session, apic_ip, node, dry_run=False):
    node_id = node["node_id"]
    pod_id = node["pod_id"]
    dn_ch = f"topology/pod-{pod_id}/node-{node_id}/sys/ch"
    dn_lsubj = f"topology/pod-{pod_id}/node-{node_id}/sys/action/lsubj-[{dn_ch}]"

    payload = {
        "actionLSubj": {
            "attributes": {"dn": dn_lsubj, "oDn": dn_ch},
            "children": [
                {
                    "eqptChReloadLTask": {
                        "attributes": {
                            "dn": f"{dn_lsubj}/eqptChReloadLTask",
                            "adminSt": "start",
                        },
                        "children": [],
                    }
                }
            ],
        }
    }
    path = f"/api/node/mo/topology/pod-{pod_id}/node-{node_id}/sys/action.json"

    if dry_run:
        print(f"  [DRY RUN] POST {protocol}://{apic_ip}{path}")
        print(f"  [DRY RUN] {json.dumps(payload, ensure_ascii=False)}")
        return True

    api_post(session, apic_ip, path, payload)
    print("\nリロードを指示しました。復帰後に --action show で currentProfile を確認してください")
    return True


# ============================================================
# main
# ============================================================


def main():
    parser = argparse.ArgumentParser(
        description="Forwarding Scale Profile の確認・適用（検証用）"
    )
    parser.add_argument("--target_node", help="対象ホスト名（--node_id 指定時は省略可）")
    parser.add_argument("--apic", required=True, help="APIC の IP / FQDN")
    parser.add_argument("--username", required=True, help="APIC ユーザ名")
    parser.add_argument(
        "--password",
        nargs="?",
        const="",
        help="APIC パスワード（値を省略すると対話入力）",
    )
    parser.add_argument("--node_id", help="ノードID（hostname を引かず直接指定する場合）")
    parser.add_argument("--pod_id", default="1", help="Pod ID（--node_id 指定時、既定 1）")
    parser.add_argument(
        "--insecure_protocol", action="store_true", help="http で接続する（既定 https）"
    )
    parser.add_argument(
        "--action",
        default="show",
        choices=["show", "apply", "reload"],
        help="show: 現状確認 / apply: プロファイル適用 / reload: 再起動",
    )
    parser.add_argument(
        "--profile", help="適用する topoctrlFwdScaleProfilePol の name"
    )
    parser.add_argument(
        "--prof_type",
        help="適用したい profType（例: highDualStack）。"
        "該当する name を APIC から逆引きする",
    )
    parser.add_argument("--dry_run", action="store_true", help="投入せず内容だけ表示")
    parser.add_argument(
        "--force", action="store_true", help="影響範囲の警告を無視して適用する"
    )
    args = parser.parse_args()

    if args.action == "apply" and not (args.profile or args.prof_type):
        print("--action apply には --profile または --prof_type が必要です")
        sys.exit(1)

    if not args.target_node and not args.node_id:
        print("--target_node または --node_id のいずれかを指定してください")
        sys.exit(1)

    global protocol
    if args.insecure_protocol:
        protocol = "http"

    username = args.username
    password = args.password if args.password else getpass.getpass("APIC password: ")

    apic_ip = args.apic
    try:
        print(f"APIC: {protocol}://{apic_ip}  (user={username})")
        session = get_session(apic_ip, username, password)
    except Exception as e:
        print(f"APIC 接続失敗: {e}")
        sys.exit(1)

    if args.node_id:
        node = get_node_by_id(session, apic_ip, args.node_id, args.pod_id)
        if not node:
            node = {
                "hostname": args.target_node or f"node-{args.node_id}",
                "node_id": args.node_id,
                "pod_id": args.pod_id,
                "role": "",
                "model": "",
                "version": "",
                "fabric_st": "",
            }
            print(f"警告: node-{args.node_id} が fabricNode に見つかりません（指定値で続行）")
    else:
        node = get_node_by_hostname(session, apic_ip, args.target_node)
        if not node or not node["node_id"]:
            print(f"{args.target_node} が fabricNode に見つかりません")
            print("  --node_id で直接指定することもできます")
            sys.exit(1)

    # 適用対象プロファイル名の解決: --profile 優先、無ければ --prof_type から逆引き
    target_profile = args.profile
    if not target_profile:
        want_type = args.prof_type
        if want_type:
            pols = list_profile_policies(session, apic_ip)
            matched = resolve_profile_name(pols, want_type)

            if not matched:
                print(f"\nprofType='{want_type}' に一致するポリシーが APIC にありません")
                if pols:
                    print("  定義済み:")
                    for p in pols:
                        print(f"    - {p.get('name', '')} (profType={p.get('profType', '')})")
                else:
                    print("  定義済みポリシーなし（Default のみ）")
                if args.action == "apply":
                    sys.exit(1)
            elif len(matched) > 1:
                print(f"\nprofType='{want_type}' に一致するポリシーが複数あります")
                for p in matched:
                    print(f"    - {p.get('name', '')}")
                print("  --profile で名前を指定してください")
                if args.action == "apply":
                    sys.exit(1)
            else:
                target_profile = matched[0].get("name", "")
                print(
                    f"\nprofType='{want_type}' -> ポリシー '{target_profile}' を使用します"
                )

    try:
        if args.action == "show":
            show(session, apic_ip, node, target_profile)
        elif args.action == "apply":
            show(session, apic_ip, node, target_profile)
            print()
            ok = apply_profile(
                session, apic_ip, node, target_profile, args.dry_run, args.force
            )
            sys.exit(0 if ok else 1)
        elif args.action == "reload":
            reload_node(session, apic_ip, node, args.dry_run)
    except Exception as e:
        print(f"エラー: {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()