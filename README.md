# nodeshut_vup.py 仕様書

## 1. 概要

Cisco ACI 環境において、APIC API 経由で Leaf / Spine ノードの切り離し（Disable）および組み込み（Enable）を自動化するスクリプト。

各ノードに対し、以下を一括で実施する：

- APIC への shutdown / noshut JSON の投入
- 投入前後の状態（admin/oper status）の採取
- 差分判定
- SSH 経由でのコマンドログ採取
- Module / Diagnostic 結果の解析
- ステータス JSON / 各種ログファイルの生成

## 2. 実行方法

### コマンドライン引数

| 引数 | 必須 | 説明 | 値 |
|---|---|---|---|
| `--target_nodes` | ◯ | 対象ホスト名（カンマ・空白区切り） | 例: `tdqntys1-Leaf01,tdqntys1-Leaf02` |
| `--pid` | ◯ | 処理 ID（ステータス／ログのキー） | 任意の文字列 |
| `--scenario_id` | ◯ | シナリオ種別 | `disable` or `enable` |
| `--type` | ◯ | ノード種別 | `leaf` or `spine` |
| `--order_group` | ◯ | オーダーグループ ID（UID） | 任意の文字列 |

### 制約

- `type=spine` の場合、`target_nodes` は 1 ノードのみ
- Leaf hostname は `Leaf` または `leaf` を含む必要あり
- Spine hostname は `SpSw` を含む必要あり

## 3. ディレクトリ構成

```
<script_directory>/
├── nodeshut_vup.py
├── credentials.py
├── result_code.py
├── apic_leafs.py
├── commands/
│   ├── leaf_commands.txt
│   ├── spine_commands.txt
│   ├── all_spine_commands.txt
│   ├── other_spine_commands.txt
│   └── check_commands.txt
├── log/<uid>/
│   ├── <pid>_status.json          # 全体ステータス
│   ├── <pid>.log                  # メインログ（STEP単位）
│   ├── <pid>_processing.log       # 処理進捗ログ
│   ├── <pid>_detail.log           # 詳細ログ
│   ├── disable/                   # disable 時の採取ファイル
│   └── enable/                    # enable 時の採取ファイル
└── run/<uid>/<hostname>/
    ├── <hostname>_shutdown.json   # shutdown 用 JSON
    ├── <hostname>_noshut.json     # noshut 用 JSON（マスター）
    ├── 01_part_<hostname>_noshut.json  # 分割パート（5行ごと）
    ├── 02_part_<hostname>_noshut.json
    └── apic_noshut.json           # APIC 片寄解除用（spine のみ）
```

## 4. ステータスコード

`result_code` モジュールで定義：

| コード種別 | 意味 |
|---|---|
| `STATUS_CODE_SUCCESS` | 全体正常終了 |
| `STATUS_CODE_SERVER_ERROR` | サーバーエラー |
| `STATUS_CODE_CLIENT_ERROR` | 入力エラー |
| `DUPLICATE_ID_CLIENT_ERROR` | UID／PID 重複 |
| `HOSTNAME_NOT_ALLOWED_CLIENT_ERROR` | hostname がDBに不在 |
| `EACH_STATUS_CODE_IN_PROGRESS` | ノード処理中 |
| `EACH_STATUS_CODE_COMPLETED` | ノード正常終了 |
| `EACH_STATUS_CODE_SERVER_ERROR` | ノード異常終了 |

`<pid>_status.json` の `each_status_code`：
- 先頭 `N` → 正常系
- 先頭 `E` → 異常系

最終的に `finalize_status()` が以下を設定：
- 全ノード成功 → `完了`
- 一部または全部失敗 → `異常終了を含む`
- 判定不能 → `不明`

## 5. シナリオ別フロー

### 5.1 Leaf Disable シナリオ（steps=3）

```
[STEP1/3] 事前確認
  ├─ APIC 接続確認（token 取得）

[STEP2/3] ノード切り離し（ノードごとに繰り返し）
  ├─ ホスト毎に token 再取得
  ├─ node_id / pod_id 取得
  ├─ create_leaf_shutdown() で shutdown/noshut JSON 生成
  │   └─ split_file_by_lines() で 5行毎に分割（enable 用）
  ├─ before 状態採取
  │   ├─ Spine 向け admin/oper（before_spine_*_statuses.json）
  │   ├─ APIC 向け admin/oper（APIC 接続ノードのみ）
  │   └─ 自ノードポート admin/oper（before_*_statuses.json）
  ├─ shutdown JSON を POST
  ├─ sleep(5) + shutdown 後 APIC token 再取得 + sleep(5)
  ├─ after 状態採取
  │   ├─ 自ノードポート（check_target=down）
  │   └─ Spine 向け（check_target=up）
  ├─ ポート Status 判定（想定外ならエラー）
  └─ SSH ログ採取（leaf_commands.txt）

[STEP3/3] 事後確認（プレースホルダ、現状は START/END ログのみ）

finalize_status()
```

### 5.2 Spine Disable シナリオ（steps=4）

```
[STEP1/4] 事前確認

[STEP2/4] APIC 片寄
  ├─ 全 Spine の SSH ログ採取（before）
  │   ├─ all_spine_commands.txt
  │   └─ check_commands.txt 内の各コマンド
  ├─ Module 確認（show module 解析）
  ├─ Diagnostic 確認（DIAG TEST SUCCESS の確認）
  ├─ create_apic_shutdown() で APIC 片寄用 JSON 生成
  ├─ before_apic_oper 状態採取
  ├─ APIC shutdown JSON を POST
  ├─ sleep(AFTER_ENABLE_DISABLE_SLEEP)
  └─ APIC ポート Status 判定
      ├─ 対象 APIC-Leaf 側ポートは down
      └─ 他 APIC-Leaf 側ポートは up

[STEP3/4] Spine 再起動 & ノード切り離し
  ├─ create_spine_shutdown() で shutdown JSON 生成
  ├─ before（spine-leaf 間、spine-自分）状態採取
  ├─ reload_node() で対象 Spine を再起動
  ├─ wait_for_spine_status(desired=inactive) ← APIC 自動選択
  ├─ APIC token 再取得
  ├─ other spine の SSH ログ採取
  ├─ spine shutdown JSON を POST
  ├─ wait_for_spine_status(desired=active) ← APIC 自動選択
  └─ APIC token 再取得

[STEP4/4] 事後確認
  ├─ 対象 spine の SSH ログ採取（spine_commands.txt）
  ├─ sleep(300)
  ├─ 全 spine の SSH ログ採取（after）
  ├─ check_commands.txt の各コマンドログ採取（after）
  ├─ before/after の SSH ログ差分判定
  │   ├─ lldp は対象 Spine では「APIC Leaf のみ表示」を確認
  │   └─ interface/isis/module/diagnostic は SSH 比較スキップ
  ├─ after 状態採取（spine 自分は down、spine-apic_leaf 間は up）
  ├─ Module / Diagnostic 確認
  └─ 全項目 OK で正常終了
```

### 5.3 Enable シナリオ（Leaf / Spine 共通、`post_threading` で並列処理）

```
[STEP1/N] 事前確認（APIC 接続）

[STEP2/N] ノード組み込み or Spine 組み込み
  └─ 各ノードに対し post_threading() をスレッド起動
       ├─ noshut パートファイルを順次 POST
       │   └─ 各 POST 間に POST_FILE_SLEEP_INTERVAL 秒待機
       ├─ spine の場合: apic_noshut.json を事後投入（APIC 片寄解除）
       ├─ sleep(AFTER_ENABLE_DISABLE_SLEEP)
       ├─ after 状態採取
       └─ disable-before と enable-after を比較
           ├─ ノード自身の admin/oper
           ├─ leaf: Spine 向け admin/oper
           ├─ leaf: APIC 向け admin/oper（APIC 接続ノードのみ）
           └─ spine: APIC 向け oper

[STEP3/N] APIC 片寄解除（spine のみ、ラベル出力のみ）

[STEP4/N] 事後確認（ラベル出力のみ）

finalize_status()
```

## 6. 主要関数

### 6.1 APIC 操作

| 関数 | 役割 |
|---|---|
| `get_token(apic_ip, ...)` | APIC ログインしてトークン取得 |
| `get_token_from_random_node(hostname)` | hostname から area_network を引き、生きている APIC を選んでトークン取得 |
| `apic_select(hostname)` | DB から area_network・APIC 候補を取得 |
| `check_connection(apic_ips)` | 候補 IP の中から到達可能なものをランダム選択 |
| `get_hostname_info(hostname, ...)` | ノードの node_id / pod_id を取得 |
| `post_file(...)` | JSON ファイルを APIC に POST（リトライ 3 回） |
| `reload_node(...)` | 対象ノードに reload を投げる |
| `wait_for_spine_status(hostname, desired_status, ...)` | Spine が active/inactive になるのを待つ（毎ループ APIC を選び直す） |

### 6.2 状態採取

| 関数 | 役割 |
|---|---|
| `get_leaf_ports(...)` | 配下/Spine 向け DN リスト取得 |
| `get_apic_ports(..., up_only)` | APIC 接続ポート DN リスト取得 |
| `get_spine_ports(...)` | LLDP から spine のポート DN リスト取得 |
| `get_leaf_admin_statuses(...)` | Leaf の adminSt 採取・JSON 出力 |
| `get_leaf_oper_statuses(...)` | Leaf の operSt 採取・JSON 出力 |
| `get_spine_admin_statuses(...)` | Spine の adminSt 採取・JSON 出力 |
| `get_spine_oper_statuses(...)` | Spine の operSt 採取・JSON 出力 |
| `get_apic_admin_statuses(...)` | APIC 側ポートの adminSt 採取 |
| `get_apic_oper_statuses(...)` | APIC 側ポートの operSt 採取 |

### 6.3 比較・解析

| 関数 | 役割 |
|---|---|
| `compare_status_reports(before, after)` | JSON ベースで before/after の差分判定 |
| `compare_ssh_logs(before, after, diff_out)` | SSH ログの行単位差分判定、unified diff 出力 |
| `analyze_show_module_log(log_path)` | `show module` 出力から異常モジュール検出 |
| `analyze_diag_result_log(log_path)` | `show diagnostic` 出力から失敗テスト検出 |
| `check_only_neighbor_from_file(path, target)` | LLDP ログから「指定ホストのみ」を確認 |

### 6.4 ファイル生成

| 関数 | 役割 |
|---|---|
| `generate_shutdown_files(ports, dir, hostname)` | shutdown/noshut JSON を生成 |
| `split_file_by_lines(path, fname, n)` | noshut JSON を n 行ごとに分割 |
| `create_leaf_shutdown(...)` | Leaf 用 shutdown ファイル一式作成 |
| `create_spine_shutdown(...)` | Spine 用 shutdown ファイル一式作成 |
| `create_apic_shutdown(...)` | APIC 片寄用 shutdown ファイル作成 |

### 6.5 ログ

| 関数 | 役割 |
|---|---|
| `log_step(path, msg)` | メインログに STEP 単位の START/END/ERROR を記録 |
| `log_processing(dir, pid, msg)` | 処理進捗を記録（標準出力にも） |
| `log_detail(dir, pid, msg)` | デバッグ詳細を記録 |
| `update_node_status(dir, uid, target, code, msg)` | `<pid>_status.json` のノード単位ステータスを更新 |
| `finalize_status(dir, uid)` | `<pid>_status.json` の全体ステータスを確定 |
| `set_client_error_status(dir, uid, hostnames, msg, code)` | 入力エラー時のステータス JSON を一括生成 |
| `fail_all_and_exit(dir, uid, hostnames, msg, code)` | 全ノードを異常終了にして sys.exit(1) |

## 7. 比較ロジックの仕様

### 7.1 ポート状態比較（`compare_status_reports`）

- 比較対象キー: `adminSt_statuses` / `operSt_statuses`
- `timestamp` / `overall_all_ports_up` などの揮発的フィールドは無視
- node_id 単位でマッチング
- ポートが before/after で増減 → 差分扱い
- ポート値が異なる → 差分扱い
- 差分が 1 件でもあれば `False` を返す

### 7.2 Enable 時の判定

#### Leaf
```
status_ok = cmp_admin and cmp_oper
            and cmp_spine_admin and cmp_spine_oper
            and cmp_apic_admin and cmp_apic_oper
```
ただし APIC 非接続のノードは `cmp_apic_admin = cmp_apic_oper = True`。
配下不在ノードは `cmp_admin = cmp_oper = True`。

#### Spine
```
status_ok = cmp_admin and cmp_oper and cmp_apic_oper
```

### 7.3 Disable Spine 事後判定

| 項目 | 期待値 |
|---|---|
| `admin_ok` | 自身の adminSt が down |
| `oper_ok` | 自身の operSt が down |
| `spine_leaf_admin_ok` | spine-apic_leaf 間 adminSt が up |
| `spine_leaf_oper_ok` | spine-apic_leaf 間 operSt が up |
| `diff_ok` | SSH ログ差分なし |
| `modules_ok` | `show module` 全て OK |
| `diag_ok` | DIAG TEST 全て SUCCESS |

全項目 OK で正常終了、いずれか NG なら `reasons` リストにメッセージを詰めてノード単位で異常終了。

## 8. エラー処理方針

| 状況 | 対応 |
|---|---|
| 入力エラー（引数不備） | `set_client_error_status` で記録 → `sys.exit(1)` |
| APIC 接続失敗（事前確認） | `fail_all_and_exit` で全体停止 |
| Disable のループ内例外 | `try/except Exception` でノード単位の異常終了、次のノードへ |
| POST 失敗 | リトライ 3 回後、ノード単位で異常終了 |
| Spine reload 失敗 | `fail_all_and_exit` で全体停止 |
| Enable の post_threading 内例外 | ノード単位の異常終了、他スレッドは継続 |

## 9. 並列処理

### 9.1 Enable（`post_threading`）
- ノードごとにスレッドを起動
- token / apic_ip / apic は呼び出し元から共有
- ステータス更新は `status_json_lock` で排他制御

### 9.2 SSH ログ採取
- Spine ごとにスレッド並列
- 例外はリスト `exceptions` に集約して join 後に判定

## 10. 設定（`credentials` モジュール想定）

| 定数 | 用途 |
|---|---|
| `PSQL_HOST` / `PSQL_DB` / `PSQL_USER` / `PSQL_PASSWORD` | PostgreSQL 接続情報 |
| `USERNAME` / `PASSWORD` | APIC / SSH 共通の認証情報 |
| `PROTOCOL` | `http` or `https` |
| `AFTER_ENABLE_DISABLE_SLEEP` | shutdown/noshut 後の待機秒 |
| `POST_FILE_SLEEP_INTERVAL` | POST 間の待機秒 |

## 11. 依存モジュール

- `requests` / `urllib3` / `paramiko` / `psycopg2`
- ローカル: `credentials` / `result_code` / `apic_leafs`

## 12. 既知の制約

- `wait_for_spine_status` は 30 分タイムアウト、毎 60 秒チェック（APIC は毎回選び直し）
- `time.sleep(300)` 等のマジックナンバーが点在しており、調整は直接コード編集が必要
- `main()` は 1400 行超でネスト深く、保守性に課題あり（リファクタ候補）
