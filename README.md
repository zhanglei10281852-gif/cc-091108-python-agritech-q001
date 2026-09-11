# 温室灌溉领域资料

这里保存园区分区、作物阶段、阀门能力和遥测事件的约定数据。时间采用带偏移量的 ISO 8601 格式，水量统一使用升，流量统一使用升每分钟。

`reference/domain.json` 中的 `zones` 描述取水分区，`valves` 给出阀门归属及额定流量，`telemetry` 展示业务发生时间与接收时间不同的记录。遥测质量分为 `good`、`suspect` 和 `offline`，接收顺序不代表实际发生顺序。

运行资料校验：

```bash
python -m unittest discover -s tests
```
