# OKX Public API Demo

这个项目演示如何使用 OKX 官方公开接口获取支持的交易项目以及实时行情数据（挂单/成交）。
代码已整合到 `src/tauto/okx.py`，并在请求失败时内置重试逻辑。

## 环境要求

- Python 3.9+

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方式

列出交易项目（现货示例）：

```bash
python main.py list --type SPOT
```

获取 BTC-USDT 实时挂单（默认深度 5 档）：

```bash
python main.py book BTC-USDT --depth 5
```

获取 BTC-USDT 最新成交（默认 100 条）：

```bash
python main.py trades BTC-USDT --limit 100
```

获取 BTC-USDT K 线（1 分钟，100 根）：

```bash
python -c "from tauto.okx import OkxClient; print(OkxClient().get_candlesticks('BTC-USDT', bar='1m', limit=100))"
```

持久化获取 K 线并写入 SQLite（秒级数据请使用 `1s` 周期，会自动切换到 OKX 的历史 K 线接口；历史数据默认 QPS 为 10，实时数据默认 QPS 为 1）：

```bash
python main.py candles BTC-USDT --bar 1s --db candles.db
```

补拉指定时间范围的历史数据并修复缺失数据：

```bash
python main.py candles BTC-USDT --bar 1s --db candles.db --start 1704067200000 --end 1704153600000
```

## 代码调用示例

```python
from tauto.okx import OkxClient
from tauto.candles import CandlestickService
from tauto.storage import SqliteCandleStore

client = OkxClient(max_retries=3, retry_backoff=0.5)
instruments = client.list_instruments("SPOT")
trades = client.get_trades("BTC-USDT", limit=100)
candles = client.get_candlesticks("BTC-USDT", bar="1m", limit=100)

store = SqliteCandleStore("candles.db")
service = CandlestickService(client=client, store=store, bar="1s")
service.initialize()
service.fetch_realtime("BTC-USDT")
service.cleanup_old_data()
```

## 测试

```bash
pytest
```

## 说明

- 使用 OKX 的公共接口，无需 API Key。
- 接口返回的 `instId` 可直接用于挂单和成交查询。
- 参考文档：https://www.okx.com/docs-v5/zh/#overview
