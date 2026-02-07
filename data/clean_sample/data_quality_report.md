# Data Quality Report

## Contents
- [Overview](#overview)
- [Metrics Summary](#metrics-summary)
- [Column Details](#column-details)

## Overview
| item | value |
| --- | --- |
| Rows | 2 |
| Columns | 20 |

## Metrics Summary
| metric | value |
| --- | --- |
| row_count_raw | 3 |
| row_count_cleaned | 2 |
| parse_rate | 100.00% |
| negotiable_rate | 0.00% |
| duplicates_rate | 33.33% |

### Key Column Missing Rate
| column | missing_rate |
| --- | --- |
| salary_months | 50.00% |
| url | 0.00% |
| title | 0.00% |
| company | 0.00% |
| city | 0.00% |
| salary_min_k | 0.00% |
| salary_max_k | 0.00% |
| salary_is_negotiable | 0.00% |
| exp_min_years | 0.00% |
| exp_max_years | 0.00% |
| edu_level | 0.00% |

## Column Details
### `url`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: https://example.com/job/1 (1); https://example.com/job/2 (1)

### `title`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: Data Engineer (1); Data Analyst (1)

### `company`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: Alpha (1); Beta (1)

### `city`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: Shenzhen (1); Shanghai (1)

### `publish_date`
- dtype: datetime64[ns]
- missing_rate: 0.00%
- distribution: min=2025-01-01 00:00:00, max=2025-01-03 00:00:00

### `salary_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 20-30K 13薪 (1); 15-20K (1)

### `exp_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 3-5年 (1); 1-3年 (1)

### `edu_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 本科 (2)

### `__source_file`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: jobs_sample.csv (2)

### `raw_salary_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 20-30K 13薪 (1); 15-20K (1)

### `fetched_at`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: UNKNOWN (2)

### `salary_min_k`
- dtype: float64
- missing_rate: 0.00%
- distribution: numeric stats: min=15.0, 50%=17.5, 90%=19.5, max=20.0

### `salary_max_k`
- dtype: float64
- missing_rate: 0.00%
- distribution: numeric stats: min=20.0, 50%=25.0, 90%=29.0, max=30.0

### `salary_months`
- dtype: float64
- missing_rate: 50.00%
- distribution: numeric stats: min=13.0, 50%=13.0, 90%=13.0, max=13.0

### `salary_is_negotiable`
- dtype: bool
- missing_rate: 0.00%
- distribution: numeric stats: 

### `exp_min_years`
- dtype: float64
- missing_rate: 0.00%
- distribution: numeric stats: min=1.0, 50%=2.0, 90%=2.8, max=3.0

### `exp_max_years`
- dtype: float64
- missing_rate: 0.00%
- distribution: numeric stats: min=3.0, 50%=4.0, 90%=4.8, max=5.0

### `edu_level`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: bachelor (2)

### `skill_tags`
- dtype: object
- missing_rate: 0.00%
- distribution: top5:  (2)

### `skill_tag_count`
- dtype: int64
- missing_rate: 0.00%
- distribution: numeric stats: min=0.0, 50%=0.0, 90%=0.0, max=0.0
