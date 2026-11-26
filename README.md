# 售电切换与计量断点对账

用电客户通过售电公司和计量方完成交接。联调区中的计量点、合同主体和计费读数均为合成。

业务记录使用 [《售电交接数据约定》](docs/exchange.md) 的格式。`data/` 中含各参与方合成单据，可由 Python 3.11.2 读取，无需第三方包。SQLite 来自 Python 自带模块。

## 交换资料

在项目根目录启动本地资料源，可按 JSON 文件名访问记录。

```sh
python3 -m http.server --bind 127.0.0.1 --directory data 8411
```

外方回执联调另由以下进程提供。请求和回执结构见交换约定。

```sh
python3 peer.py --port 8412
```

## 启动交接服务

服务只依赖标准库，提供 Python API、WSGI 入口和 JSON HTTP 服务。首次启动会把 `data/` 中的合成计量点、费率、申报单和读数装入 SQLite：

```sh
python3 service.py \
  --host 127.0.0.1 --port 8410 \
  --db switch_service.sqlite3 \
  --data data \
  --peer-url http://127.0.0.1:8412
```

也可使用 `python3 app.py`，或在 WSGI 服务器中加载 `app:application`。

HTTP 调用用请求头表示操作身份：

```http
X-Actor-Type: customer
X-Actor-Id: 桃源板材
```

售方财务使用 `X-Actor-Type: finance`（或 `retailer`）和 `X-Actor-Retailer: 松江零售`；计量回告使用 `X-Actor-Type: meter`。本机受信任脚本可使用 `X-Actor-Type: system`。未标明身份的 `/api/*` 请求返回 `401`。

### 常用接口

| 方法与路径 | 用途 |
| --- | --- |
| `POST /api/switches` | 购电人确认换签、申报计划日和计量边界；可带 `master_agreement_id` |
| `POST /api/switches/{id}/agree` | 旧/新售方确认按某日切换 |
| `POST /api/switches/{id}/commitment` | 前序归属查清后发出新承诺 |
| `POST /api/switches/{id}/submit` | 向联调计量接口发送 `{meter_ids, action:"apply"}` |
| `POST /api/switches/{id}/poll` | 查询并幂等处理计量事件 |
| `POST /api/switches/{id}/withdraw` | 撤回尚未生效或与生效回执交叉的换签 |
| `POST /api/switches/{id}/decision` | both 组部分通过后，决定剩余点 `continue`/`stop` |
| `POST /api/events` | 直接回告计量事件 |
| `POST /api/readings` | 回告/更正读数 |
| `GET /api/attribution?meter_id=...&at=...` | 查前一个有效归属；未终局时为 `undetermined` |
| `GET /api/ownership?meter_id=...&at=...` | 查某时刻商业责任方 |
| `GET /api/settlement?meter_id=...` | 按相邻读数、覆盖时段和商业归属计算 |
| `GET /api/settlement-scope?retailer=...&meter_id=...` | 旧方期末范围或新方第一期范围 |
| `GET /api/voucher?meter_id=...` | 唯一交界凭证，期末/期初使用同一累计表码 |
| `POST /api/bills/generate` | 生成或刷新草稿账单 |
| `POST /api/bills/{id}/issue` | 签发账单并冻结依据 |
| `POST /api/adjustments/{id}/acknowledge` | 认领/确认计量更正产生的差额 |
| `GET /api/finance?retailer=...` | 售方财务只能查自己的账单、计量和费率 |
| `GET /api/status-board?customer=...` | 客户查看申报中、延期、临时承接、已确认等状态 |
| `GET /api/trace/{meter_id}` | 企业串联责任、读数、交界、账单和差额凭证 |

无 Socket 的调用方式与 HTTP 共用同一套路由：

```python
from service import SwitchService, Api

service = SwitchService("switch_service.sqlite3", data_dir="data")
api = Api(service)
status, body = api.dispatch("GET", "/api/trace/E-1", headers={
    "X-Actor-Type": "customer",
    "X-Actor-Id": "桃源板材",
})
```

## 核心业务规则

- **受理、约定、生效分开**：`accepted` 只表示计量方受理；双方确认计划日只形成合同承诺；只有 `applied` + 有效交界读数才建立物理交界。
- **无交界不计费转移**：交界未成立前，`ownership_at` 仍返回原方，不会旧方和新方同时计价。
- **计量点不是全户开关**：`CH-14` 的 `E-1` 通过、`E-2` 拒绝/撤回时，只有 `E-1` 建立交界；申报单状态为 `partial_needs_decision`，`E-2` 仍归松江零售。
- **both 组临时承接**：`E-1` 从物理交界进入 `bridge_terms`，样例为松江零售按 55 分/kWh 临时承接；最后一点成功时才按最后成功点确立正式商业切换日。剩余点不再办理时，临时承接持续到客户另签接收约定。
- **撤回/生效交叉按计量方事序判断**：同一 `change_id + meter_id` 以计量时间和 `revision` 排序。`already-applied` 的撤回拒绝不会推翻已生效交界；重复或旧的 `accepted` 也不会撤销事实。
- **前序归属未定时阻断新承诺**：若前一笔已过计划日但只有 `accepted`，`resolve_attribution(...).status == "undetermined"`；新申请可留档，但 `commitment_issued=false`，不能把错误售方写成起点。已确认交界之后的新申请只能判断更晚范围，不能改写早期归属。未结清账单本身不阻止新申请。
- **唯一用量断点**：交界凭证的 `closing.end_kwh` 与 `opening.start_kwh` 是同一个认可表码，不会把两个“版本差”重复计成新增用量。
- **更正与签发分离**：未签发账单按最新 confirmed 读数重算；已签发账单保持不变，而是为相关售方生成可认领的 `Adjustment`。两边都取得依据并确认后，交界对账才为 `balanced`。更正只改读数差额，不改变归属，也不会把客户换回旧方。
- **重启和重复报文安全**：事实、事件 ID、读数 ID 和幂等键保存在 SQLite；重复事件不会生成第二个交界或重复调整。

### 样例中的确定结果

处理 `data/peer.json` 的 `CH-14` 回执后，`E-1` 的正式读数版本从 `R-B1=13150` 更正为 `R-B2=13163`：

- `2027-02-01 → 2027-02-02`：163 kWh，归松江零售原合同，单价 50 分，金额 8150 分；
- `2027-02-02 → 2027-02-03`：117 kWh，归松江零售临时承接，单价 55 分，金额 6435 分；
- `E-2`：`2027-02-01 → 2027-02-03` 的 320 kWh 仍按松江零售 50 分/kWh 结算，金额 16000 分。

若先按 `R-B1` 签发账单、后收到 `R-B2`，会形成两边差额：旧合同期末侧 `+13 kWh / +650 分`，交界后期初侧 `-13 kWh`；正式合同费率为 63 分时对应 `-819 分`，临时承接费率为 55 分时对应 `-715 分`。

## 资料校验与服务测试

```sh
python3 -m unittest discover -s tests -v
```

`tests/test_service.py` 覆盖 CH-14/CH-15 联调报文、断连后查询、乱序/重复事件、部分通过、临时承接、归属阻断、历史交界保护、读数更正、已签发账单调整、财务隔离、SQLite 重启和 HTTP/WSGI 入口。
