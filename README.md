# DataLab

DataLab 是一个面向初学者的招聘数据小项目，目标是把“网页职位信息”变成“可分析报告”：
- 从招聘网站抓取原始数据
- 清洗并结构化 JD 字段
- 输出数据质量报告和市场分析报告

## 1）环境安装

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
```

## 2）一键运行（推荐）

只需要粘贴一个猎聘职位列表链接：

```bash
python -m datalab.jd.oneclick --url "https://www.liepin.com/career/dianziruanjian/" --pages 2 --output-dir data/oneclick_liepin
```

运行完成后会生成：
- `data/oneclick_liepin/raw_crawled.csv`
- `data/oneclick_liepin/cleaned.parquet`
- `data/oneclick_liepin/data_quality_report.md`
- `data/oneclick_liepin/jd_market_report.md`

## 3）分步运行（便于学习流程）

### 3.1 抓取原始数据

```bash
python -m datalab.jd.crawl --seed-url "https://www.liepin.com/career/dianziruanjian/pn{page}/" --pages 2 --output data/raw/liepin_jobs.csv --selector "card=.job-card-pc-container" --selector "url=.job-detail-box > a[href]" --selector "title=.job-title-box .ellipsis-1" --selector "company=.company-name" --selector "city=.job-dq-box .ellipsis-1" --selector "salary_text=.job-salary" --selector "exp_text=.job-labels-box .labels-tag:nth-of-type(1)" --selector "edu_text=.job-labels-box .labels-tag:nth-of-type(2)"
```

### 3.2 清洗数据

```bash
python -m datalab.clean --input data/raw/liepin_jobs.csv --output data/clean_liepin
```

### 3.3 生成市场分析报告

```bash
python -m datalab.jd.analyze --input data/clean_liepin/cleaned.parquet --output data/clean_liepin/jd_market_report.md
```

## 4）JD 特征字段映射

- `salary_text` -> `salary_min_k`, `salary_max_k`, `salary_months`, `salary_is_negotiable`
- `exp_text` -> `exp_min_years`, `exp_max_years`
- `edu_text` -> `edu_level`

## 5）运行测试

```bash
python -m pytest -q
```

## 6）发布到 GitHub

```bash
git init
git add .
git commit -m "feat: datalab one-click jd pipeline"
git branch -M main
git remote add origin <你的仓库地址>
git push -u origin main
```
