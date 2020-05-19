[![Gitpod Ready-to-Code](https://img.shields.io/badge/Gitpod-Ready--to--Code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/chandlersong/greenseer) 

# Green Seer

[EN](README_en.md)

主要是用来分析财务数据的一个工具。

## dataset

A股的财务报表数据

### API:

- `greenseer.dataset.china_dataset.fetch_one_report(stock_ids)`: 一只股票的财务报表
- `greenseer.dataset.china_dataset.fetch_multi_report(stock_ids)`: 多只股票的财务报表
- `greenseer.dataset.china_dataset.create_targets(stock_ids)`: 制造目标(我不太确定target是否翻译成目标)
    - 参数是一个字典，然后输出下面会列出，key会成为列名，value是stock id的数组。这样的设计是为了动态。觉得股票分析的时候，目标很难统一
    - create_default_targets： 默认的目标。现在只有st股票
    
### 功能：

- 默认情况下，所有的数据都会下载在 *reportData* 所列的文件夹
    - 可以使用 `greenseer.dataset.china_dataset.set_local_path` 改变路径
- 可以用 'force_remote' 强制远程读取
    - 建议出新报表的时候，再使用这个强制更新

数据格式:

- 会把所有报表的科目进行平铺

| 股票编号 | 日期 | 科目1 |  科目2|
| :-----| ----:    | ----:   | ----:    |
| id1   | 2019-09  |   1     |   2      |
|       | 2019-06  | 2       |   4      |
| id2   | 2019-09  | 3       |   7      |
|       | 2019-06  | 4       |   8      |


target 格式:

| 股票编号 | st |  50|
| :-----| ----:   | ----:    |
| id1   |   1     |   0      |
| id2   |   0     |   1      |
| id3   |   0     |   0      |