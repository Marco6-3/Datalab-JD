# Data Quality Report

## Overview
- Rows: 2
- Columns: 16

## Metrics Summary
- parse_rate: 1.0000
- negotiable_rate: 0.0000
- duplicates_rate: 0.3333

### Key Column Missing Rate
| column | missing_rate |
| --- | --- |
| city | 0.0000 |
| company | 0.0000 |
| edu_level | 0.0000 |
| exp_max_years | 0.0000 |
| exp_min_years | 0.0000 |
| salary_is_negotiable | 0.0000 |
| salary_max_k | 0.0000 |
| salary_min_k | 0.0000 |
| salary_months | 0.5000 |
| title | 0.0000 |
| url | 0.0000 |

## Column Details
### `url`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: https://example.com/job/1 (1), https://example.com/job/2 (1)

### `title`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: Data Engineer (1), Data Analyst (1)

### `company`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: Alpha (1), Beta (1)

### `city`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: Shenzhen (1), Shanghai (1)

### `publish_date`
- dtype: datetime64[ns]
- missing_rate: 0.00%
- distribution: min=2025-01-01 00:00:00, max=2025-01-03 00:00:00

### `salary_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 20-30K 13薪 (1), 15-20K (1)

### `exp_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 3-5年 (1), 1-3年 (1)

### `edu_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 本科 (2)

### `__source_file`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: jobs_sample.csv (2)

### `salary_min_k`
- dtype: float64
- missing_rate: 0.00%
- distribution: numeric stats: count=2.0, mean=17.5, std=3.5355, min=15.0, 25%=16.25, 50%=17.5, 75%=18.75, max=20.0

### `salary_max_k`
- dtype: float64
- missing_rate: 0.00%
- distribution: numeric stats: count=2.0, mean=25.0, std=7.0711, min=20.0, 25%=22.5, 50%=25.0, 75%=27.5, max=30.0

### `salary_months`
- dtype: float64
- missing_rate: 50.00%
- distribution: numeric stats: count=1.0, mean=13.0, min=13.0, 25%=13.0, 50%=13.0, 75%=13.0, max=13.0

### `salary_is_negotiable`
- dtype: bool
- missing_rate: 0.00%
- distribution: numeric stats: count=2, unique=1, top=0, freq=2

### `exp_min_years`
- dtype: float64
- missing_rate: 0.00%
- distribution: numeric stats: count=2.0, mean=2.0, std=1.4142, min=1.0, 25%=1.5, 50%=2.0, 75%=2.5, max=3.0

### `exp_max_years`
- dtype: float64
- missing_rate: 0.00%
- distribution: numeric stats: count=2.0, mean=4.0, std=1.4142, min=3.0, 25%=3.5, 50%=4.0, 75%=4.5, max=5.0

### `edu_level`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: bachelor (2)
