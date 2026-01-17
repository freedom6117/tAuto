# OKX Public API Demo

这个项目演示如何使用 OKX 官方公开接口获取支持的交易项目以及实时行情数据（挂单/成交）。

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

## 说明

- 使用 OKX 的公共接口，无需 API Key。
- 接口返回的 `instId` 可直接用于挂单和成交查询。
- 参考文档：https://www.okx.com/docs-v5/zh/#overview
