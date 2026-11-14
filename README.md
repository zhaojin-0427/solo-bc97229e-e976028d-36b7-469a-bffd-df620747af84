# 病理样本跨组泄漏核查系统

病理模型数据工程师在交付**样本清单**前，用本系统排除**同源切片跨训练 / 验证 / 测试组**
的数据泄漏。系统提供 FastAPI + SQLite 的核查 API：上传 LIMS 衍生关系与分组，
将衍生关系视为**无向图**做连通性核查，同一连通分量内若出现不同组别即判定泄漏，
返回跨组样本及一条**实际连接路径**；仅当前完整数据版本核查通过后才允许下载分组 CSV。

## 一、快速启动（Docker Compose）

```bash
docker compose up --build
```

启动后：

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 交互式控制台 | http://localhost:8080/ | 上传、轮询进度、查看冲突路径、下载放行 CSV |
| Swagger 文档 | http://localhost:8080/docs | 可直接 Try it out 的交互 API 文档 |
| 健康检查 | http://localhost:8080/health | 返回状态与 worker 标识 |

编排包含两个对等 API 进程（`api1`、`api2`）和一个 Nginx 轮询负载均衡，
三者共享同一挂载卷上的 SQLite 文件，用于真实演示多进程一致性。

### 不用 Docker 的本地运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
LEAKCHECK_DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
# 一键端到端冒烟演示（自动起服务、上传、核查、下载）
python scripts/smoke_demo.py
```

## 二、CSV 格式

**衍生关系**（只增、重复合一，整批提交），表头支持中英文：

```csv
原样本号,衍生样本号
S-1001,D-2001
D-2001,E-3001
```

**分组**（整份替换，同一样本仅允许一组）：

```csv
样本号,组别
S-1001,train
D-2001,train
```

样例文件见 `demo/`：`relations_clean.csv`、`groups_clean.csv`（无泄漏）、
`groups_leak.csv`（含跨组冲突）。

## 三、API 一览

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/api/upload/relations` | 上传衍生关系（追加、重复合一、整批事务） |
| POST | `/api/upload/groups` | 上传分组（整份替换、整批事务） |
| GET | `/api/status` | 当前版本、放行状态、最新核查进度、下载可用性 |
| GET | `/api/runs/latest` | 最新核查任务（失败时含冲突与路径明细） |
| GET | `/api/runs/{run_id}` | 指定核查任务进度 |
| GET | `/api/versions` | 数据版本账本 |
| GET | `/api/release/groups.csv` | 下载冻结的已通过分组 CSV（未通过返回 409） |

## 四、需求如何被满足

### 1. 数据模型与提交语义

- **关系只增、重复合一**：`derivation_edge` 以 `(原样本号, 衍生样本号)` 为唯一键，
  `INSERT OR IGNORE` 合重，响应分别返回新增行数与合一行数。
- **分组整份替换**：每次上传生成新的 `group_batch`，旧批次保留历史但不再生效，
  “当前分组”始终只指向最新批次。
- **两类上传均整批提交**：CSV 先全量解析校验（空值、列数、同一样本多组等），
  任一非法行整体回滚，不会半份落库。

### 2. 无向连通性核查与冲突路径

- 每条关系在工作表中按**双向**存储，天然作为无向边。
- 采用磁盘支撑的 BFS 求连通分量；同一分量内出现 ≥2 个不同组别即冲突，
  返回跨组的两个样本、各自组别、分量大小，以及一条沿 BFS 父指针树还原的
  **真实连接路径**（每对相邻节点都对应真实边）。
- 未分组样本视为**中性节点**，不参与跨组判定。
- 仅依据**已入库关系**判定；上传事务未提交的数据对核查不可见。

### 3. 版本绑定与放行（结论绑定完整数据版本）

- 每次提交在 `data_version` 账本追加一个单调递增版本号；**每个核查 run 都绑定
  其提交时的确切版本**，并按该版本的历史快照（`edge.version <= ?` 与对应分组批次）
  重建图，核查期间新上传不影响在途 run。
- 核查完成时若发现当前版本已更新，结论被标记 `superseded` 并**丢弃**，
  迟到核查不会覆盖新状态、不会错误放行新版本。
- 任何一次变更提交都在同一事务内把旧 `acceptance` 置为 `superseded`，
  **即提交即撤销旧放行**；未完成核查前状态接口与下载都不报告“安全”。

### 4. 不占业务库写事务、不全图入内存

- 物理拆库：`business.db`（关系、分组、版本、放行）与 `check.db`
  （核查任务、进度、冲突、BFS 工作表）。后台核查只写核查库，
  业务库以只读 URI 打开，**不持有业务库写事务**，核查期间上传与查询照常。
- 邻接表、BFS 前沿、父指针全部是 SQLite 工作表；Python 侧仅持有批次（2000 行）
  与单条还原路径，并对 page cache 设软上限。实测 20 万节点 / 20 万边核查，
  进程峰值 RSS 约 90 MB，图与全部搜索状态均在磁盘。

### 5. 多进程共享、失败重启自动重核

- 两个 API 进程共享同一卷上的库文件（WAL 模式）。任务领取用
  `BEGIN IMMEDIATE` 立即写锁 + 条件 `UPDATE`，保证同一 run 全局只被一个 worker 领取。
- worker 用心跳续约；心跳超时的僵死 run 由存活进程回收重核，
  超过 `max_attempts` 才置失败，**失败或重启都不会恢复旧放行**。
- 启动时 `reconcile` 检查最新版本是否存在 run，缺失则补核最新状态；
  `failed`/`passed` 是不可变版本上的终态，不会被循环重核。

### 6. 下载冻结快照

- 核查通过且仍是当前版本时，在受理时刻把完整分组写成**冻结快照文件**并登记；
- 下载只服务该冻结文件，途中任何变更都不会混入，响应头带 `X-Release-Version`；
- 无有效放行（未核查、核查失败、或通过后又有新提交）一律 `409`。

## 五、测试

```bash
pip install pytest httpx
python -m pytest tests/ -q
```

22 个用例覆盖：整批原子回滚、重复合一、整份替换、无向冲突与真实路径、
版本绑定与迟到结论丢弃、提交即撤销放行、冻结下载、磁盘 BFS 大图正确性、
双 worker 抢占、僵死心跳回收、重启对账等。

## 六、目录结构

```
app/
  config.py              环境变量配置
  database.py            双库连接、WAL、建表 DDL
  errors.py              领域异常到 HTTP 状态码映射
  models.py              Pydantic 模型
  repositories/
    business.py          业务库：关系/分组/版本/放行/快照
    check.py             核查库：排队、抢占、心跳、冲突持久化
  services/
    csv_parse.py         不可信 CSV 的严格解析与校验
    verifier.py          磁盘支撑的无向图 BFS 核查器
    worker.py            后台调度 worker（对账/领取/重试）
    main_service.py      应用服务编排
  routers/               上传、状态、下载路由
  main.py                FastAPI 应用工厂
demo/                    交互控制台页面与样例 CSV
scripts/smoke_demo.py    真实起服的端到端冒烟脚本
nginx/                   双进程轮询负载均衡配置
tests/                   pytest 测试套件
```
