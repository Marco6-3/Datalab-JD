# Data Quality Report

## Overview
- Rows: 2
- Columns: 16

## Column Details
### `url`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: https://jobs.example.com/1 (1), nan (1)

### `title`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: Data Engineer (1), ML Engineer (1)

### `company`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: ACME (1), ByteData (1)

### `city`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: Shenzhen (1), Shanghai (1)

### `publish_date`
- dtype: datetime64[ns]
- missing_rate: 0.00%
- distribution: min=2025-09-15 00:00:00, max=2025-10-01 00:00:00

### `salary_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 20-30K 13薪 (1), 面议 (1)

### `exp_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 3-5年 (1), 经验不限 (1)

### `edu_text`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: 本科 (1), 硕士 (1)

### `__source_file`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: sample.csv (2)

### `salary_min_k`
- dtype: float64
- missing_rate: 50.00%
- distribution: numeric stats: count=1.0, mean=20.0, min=20.0, 25%=20.0, 50%=20.0, 75%=20.0, max=20.0

### `salary_max_k`
- dtype: float64
- missing_rate: 50.00%
- distribution: numeric stats: count=1.0, mean=30.0, min=30.0, 25%=30.0, 50%=30.0, 75%=30.0, max=30.0

### `salary_months`
- dtype: float64
- missing_rate: 50.00%
- distribution: numeric stats: count=1.0, mean=13.0, min=13.0, 25%=13.0, 50%=13.0, 75%=13.0, max=13.0

### `salary_is_negotiable`
- dtype: boolean
- missing_rate: 0.00%
- distribution: numeric stats: count=2, unique=2, top=0, freq=1

### `exp_min_years`
- dtype: float64
- missing_rate: 50.00%
- distribution: numeric stats: count=1.0, mean=3.0, min=3.0, 25%=3.0, 50%=3.0, 75%=3.0, max=3.0

### `exp_max_years`
- dtype: float64
- missing_rate: 50.00%
- distribution: numeric stats: count=1.0, mean=5.0, min=5.0, 25%=5.0, 50%=5.0, 75%=5.0, max=5.0

### `edu_level`
- dtype: object
- missing_rate: 0.00%
- distribution: top5: bachelor (1), master (1)
