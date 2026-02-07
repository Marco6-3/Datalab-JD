# Data Quality Report

## Contents
- [Overview](#overview)
- [Metrics Summary](#metrics-summary)
- [Column Details](#column-details)

## Overview
| item | value |
| --- | --- |
| Rows | 40 |
| Columns | 20 |

## Metrics Summary
| metric | value |
| --- | --- |
| row_count_raw | 40 |
| row_count_cleaned | 40 |
| parse_rate | 97.50% |
| negotiable_rate | 2.50% |
| duplicates_rate | 0.00% |

### Key Column Missing Rate
| column | missing_rate |
| --- | --- |
| salary_months | 52.50% |
| exp_max_years | 27.50% |
| exp_min_years | 5.00% |
| salary_min_k | 2.50% |
| salary_max_k | 2.50% |
| url | 0.00% |
| title | 0.00% |
| company | 0.00% |
| city | 0.00% |
| salary_is_negotiable | 0.00% |
| edu_level | 0.00% |

## Column Details
### `url`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: https://www.liepin.com/job/1976872959.shtml (1); https://www.liepin.com/job/1976108383.shtml (1); https://www.liepin.com/job/1975990959.shtml (1); https://www.liepin.com/job/1975397497.shtml (1); https://www.liepin.com/job/1975220965.shtml (1)

### `title`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 嵌入式软件工程师 (10); 嵌入式软件开发工程师 (2); 嵌入式软件开发 (2); 嵌入式软件工程师（驱动开发） (1); PON产品嵌入式软件工程师 (1)

### `company`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 某知名公司 (4); 爱普拉新能源技术 (2); 某深圳大型金属制品公司 (2); 深德科 (1); 贝尔生物 (1)

### `city`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 上海-漕河泾 (2); 龙华区 (2); 东莞 (2); 南山区 (2); 上海 (2)

### `publish_date`
- dtype: float64
- missing_rate: 0.00%
- distribution: numeric stats: min=0.0, 50%=0.0, 90%=0.0, max=0.0

### `salary_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 15-25k·14薪 (2); 12-18k (2); 35-50k (2); 20-35k (2); 15-23k·13薪 (1)

### `exp_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 3-5年 (20); 5-10年 (5); 1-3年 (3); 2年以上 (2); 5年以上 (2)

### `edu_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 本科 (32); 硕士 (4); 大专 (3); 学历不限 (1)

### `__source_file`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: liepin_jobs.csv (40)

### `raw_salary_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 15-25k·14薪 (2); 12-18k (2); 35-50k (2); 20-35k (2); 15-23k·13薪 (1)

### `fetched_at`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: UNKNOWN (40)

### `salary_min_k`
- dtype: float64
- missing_rate: 2.50%
- distribution: numeric stats: min=10.0, 50%=18.0, 90%=36.0, max=40.75

### `salary_max_k`
- dtype: float64
- missing_rate: 2.50%
- distribution: numeric stats: min=14.0, 50%=30.0, 90%=53.0, max=71.0

### `salary_months`
- dtype: float64
- missing_rate: 52.50%
- distribution: numeric stats: min=13.0, 50%=14.0, 90%=16.2, max=17.0

### `salary_is_negotiable`
- dtype: bool
- missing_rate: 0.00%
- distribution: numeric stats: 

### `exp_min_years`
- dtype: float64
- missing_rate: 5.00%
- distribution: numeric stats: min=1.0, 50%=3.0, 90%=5.0, max=6.0

### `exp_max_years`
- dtype: float64
- missing_rate: 27.50%
- distribution: numeric stats: min=3.0, 50%=5.0, 90%=10.0, max=10.0

### `edu_level`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: bachelor (32); master (4); associate (3); no_requirement (1)

### `skill_tags`
- dtype: object
- missing_rate: 0.00%
- distribution: top5:  (40)

### `skill_tag_count`
- dtype: int64
- missing_rate: 0.00%
- distribution: numeric stats: min=0.0, 50%=0.0, 90%=0.0, max=0.0
