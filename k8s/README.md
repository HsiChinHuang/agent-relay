# Kubernetes 部署（Q5）

kind 叢集名稱 `relay`（context `kind-relay`），節點映像**釘選** `kindest/node:v1.34.11`
（依 digest，取自 kind v0.33.0 官方 release notes 支援清單，**不用 `:latest`**）。

## 檔案

| 檔案 | 內容 |
|---|---|
| `postgres.yaml` | Secret、PVC（1Gi RWO）、PostgreSQL **Deployment**、Service（port **5432**）、readiness + liveness probe |
| `app.yaml` | Agent Relay **Deployment**（replicas=2）、Service、initContainer 等待 Gate |

## Q5 問題對照

- **哪个资源负责保持副本数与滚动更新**：`Deployment`（`kubectl rollout status`
  等的就是它；`kubectl set image deployment/agent-relay agent-relay=agent-relay:v2`
  觸發滾動更新）。
- **如何在瀏覽器開啟 dashboard**：`kubectl port-forward svc/agent-relay <local>:8000`
  再開 `http://127.0.0.1:<local>/`（本機實測用 8084，原因見下方坑 3）。
- **如何確認重啟 Pod 後 DB 持久**：刪 postgres pod 後比較
  `select system_identifier from pg_control_system()` 與資料列——實測
  `7686464570094825511` 前後相同、資料逐欄相同、Pod 名稱改變
  （`…-bxg6p` → `…-8q5d8`），證明是同一個 PVC 而非新 initdb。

## PostgreSQL 用 Deployment + PVC，不是 StatefulSet——取捨

Q5 要教的資源是 **Deployment**（維持副本數、管理滾動更新的就是它），
Deployment + 單一 RWO PVC 已滿足「持久化 DB 儲存」。
**真正的生產部署會用 StatefulSet**：穩定 Pod 身分、有序滾動、每副本獨立
volumeClaimTemplates。此處讓步的代價已明講在 manifest 內，不是留給讀者猜：

1. `strategy: Recreate`——Deployment 預設的 RollingUpdate 會讓兩個 pod 短時
   共用同一個 RWO PVC，兩個 PostgreSQL 寫同一資料目錄是資料損毀，不是滾動。
2. `replicas: 1` 由單一 RWO PVC 硬性決定。
3. Pod 名稱每次重建都變（無穩定身分）——對單寫者情境可接受。

## 部署流程

```bash
docker build -t agent-relay:v1 .
kind load docker-image agent-relay:v1 --name relay
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/app.yaml
kubectl rollout status deployment/postgres
kubectl rollout status deployment/agent-relay
```

鏡像 tag 規則（與 Q6 同規則）：**每個版本唯一 tag**（v1、v2…），不用 `:latest`；
`imagePullPolicy: IfNotPresent` 安全的前提就是「新 tag 必然解析到新鏡像」。
改 tag 後記得重新 `kind load docker-image`，否則節點沿用舊 digest 的同名鏡像。

## 三個踩過的坑（都測過才寫在這裡）

1. **app pods 會 crash-loop（本 manifest 的 initContainer 就是教訓）**：
   第一次部署沒有 initContainer，兩個 app pod 各崩 2、3 次
   （`psycopg.OperationalError` → exit 1），K8s 沒有 compose 的
   `depends_on: service_healthy`，最終自愈但形態不對。`wait-for-postgres`
   initContainer 移植 Stage E 的健康檢查教訓：`pg_isready` **必須 `-h` 強制走
   TCP**——走 unix socket 時 initdb 的臨時伺服器（只聽 socket）會提前報健康。
2. **`$(POSTGRES_PASSWORD)` 的展開依賴 env 宣告順序**：K8s 只展開「已宣告在
   前面」變數的引用，Secret 引用必須排在 URL 之前，錯序時密碼會是字串
   `$(POSTGRES_PASSWORD)`，報錯看起來像 DB 問題。實測驗證：
   `printenv RELAY_DATABASE_URL` 顯示已展開。
3. **loopback 矩陣（全部實測，見 artifact `stage-q5-f0b`/`f0c`）**：

   | 發起方 → 目標 | `127.0.0.1:60535`（kind API） | `host.docker.internal:60535` | `127.0.0.1:8084`（kubectl.exe port-forward） |
   |---|---|---|---|
   | WSL bash | 200 OK（kind 發布的埠） | 失敗 | 拒絕連線（listener 在 Windows 側） |
   | Windows（curl.exe） | 通 | —（不適用） | 200 OK |
   | Docker bridge 容器 | 拒絕連線 | 200 OK | 拒絕連線 |
   | Docker `--network host` 容器 | 200 OK | — | — |

   結論：WSL 的 127.0.0.1 只保證能到 **Docker 發布**的埠（8081/8083…）；
   port-forward 的 listener 在 Windows loopback，WSL 連不到——dashboard 要用
   `curl.exe`，8081 誤判事件（把 Stage G SQLite 容器當成 cluster）就是混用
   兩個 127.0.0.1 的直接後果。同理 `--kubeconfig` 必須用 UNC 路徑
   `\\wsl.localhost\Ubuntu-24.04\home\te\.kube\config`，POSIX 路徑會得到
   Win32 `GetFileAttributesEx` 錯誤。

## SQLite 移植須知（本叢集用 PostgreSQL，但規則留給移植者）

本部署走 PostgreSQL，WAL 議題不適用；但若有人把 `RELAY_DATABASE_URL` 換成
SQLite 掛 PVC：SQLite 的 `-wal` 檔未 fold 回主檔前，**任何「只備份單一 .db
主檔」的備份/PVC 快照都會漏掉尚未 checkpoint 的資料**（Stage F 實測：主檔
64KB、WAL 檔 815KB）。正確做法：`sqlite3 .backup` 或備份 API；手動複製必須
連 `-wal`/`-shm` 一起帶，或先 `truncate` checkpoint。

## 驗收（本機 kind 實測結果見 `../hw3-verify-artifacts/stage-q5-*.txt`）

- `GET /` 經 port-forward → 200、`<h1>Agent Relay</h1>`
- Q2 全流程 pod 內打 `svc/agent-relay:8000` → completed（含 psql 交叉核對：
  cluster PG 有該筆、Stage G 的 SQLite volume 沒有）
- PVC 持久性：見上《Q5 問題對照》第三條
- app Deployment replicas=2：兩個 pod 共用一個 PostgreSQL，正是行鎖程式碼
  要的多 worker 型態（Stage D 紅線：FOR UPDATE 保正確、SKIP LOCKED 是吞吐
  最佳化、`uq_attempt_task_number` 是最後一道保險，改副本數前請先讀
  Stage D 結論）
