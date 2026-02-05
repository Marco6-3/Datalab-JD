# DataLab

DataLab 是一个招聘数据流水线项目，支持：
- 抓取职位数据
- 清洗并结构化字段
- 生成质量与市场分析报告

当前命令（保持兼容）：
- `python -m datalab.jd.crawl`
- `python -m datalab.clean`
- `python -m datalab.jd.analyze`
- `python -m datalab.jd.oneclick`

## 快速开始

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
```

## 一键运行

```bash
python -m datalab.jd.oneclick --url "https://www.liepin.com/career/dianziruanjian/" --pages 1 --output-dir data/oneclick_liepin
```

输出文件：
- `raw_crawled.csv`
- `cleaned.parquet`
- `metrics.json`（PR2 新增）
- `data_quality_report.md`（含 metrics 摘要）
- `jd_market_report.md`

## 配置系统（PR1）

- 默认配置：`config/config.yaml`
- 环境变量样例：`.env.example`
- 优先级：`config.yaml < env < CLI`

## 爬虫选择器（已修复猎聘 0 条问题）

`crawl` 选择器优先级：
1. CLI：`--selector key=value`
2. 配置：`config/config.yaml` -> `crawl.selectors`
3. 内置站点预设（当前含 `liepin.com`）
4. 通用默认值

## Metrics（PR2）

每次 `clean` 运行会输出 `metrics.json`，包含：
- `parse_rate`
- `missing_rate`（每个关键列）
- `negotiable_rate`
- `duplicates_rate`
- `row_count_raw` / `row_count_cleaned`

并在 `data_quality_report.md` 里新增 `Metrics Summary` 小节。

## 脚本清单（无 Makefile）

```bash
# 1) 抓取（走配置）
python -m datalab.jd.crawl --config config/config.yaml

# 2) 清洗
python -m datalab.clean --input data/raw/crawled_jobs.csv --output data/clean_liepin

# 3) 分析
python -m datalab.jd.analyze --input data/clean_liepin/cleaned.parquet --output data/clean_liepin/jd_market_report.md

# 4) 一键
python -m datalab.jd.oneclick --url "https://www.liepin.com/career/dianziruanjian/" --pages 1 --output-dir data/oneclick_liepin

# 5) 测试
python -m pytest -q
```

## 示例数据

- `data/sample/jobs_sample.csv`

