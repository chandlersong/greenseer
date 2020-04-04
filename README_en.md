# Green Seer

it's a tool for analysis fiance data.


## dataset

currently it only support data from chinese stock market company the data format is follows

support methods:

- ` greenseer.dataset.china_dataset.fetch_one_report(stock_ids)`: fetch one stock reports
- ` greenseer.dataset.china_dataset.fetch_multi_report(stock_ids)`: fetch multi reports

feature

- it will automatically fetch data and keep it in local folder *reportData*
    - you can use `greenseer.dataset.china_dataset.set_local_path` change the position in global scope
- can use 'force_remote' to force get the data from remote and refresh local cache
    - please update when there's new report.

data format:

- it won't group subject by report type

| stockid | sesion | subject |  subject2|
| :-----| ----:    | ----:   | ----:    |
| id1   | 2019-09  |   1     |   2      |
|       | 2019-06  | 2       |   4      |
| id2   | 2019-09  | 3       |   7      |
|       | 2019-06  | 4       |   8      | 