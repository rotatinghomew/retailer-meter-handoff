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

## 资料校验

```sh
python3 -m unittest discover -s tests -v
```
