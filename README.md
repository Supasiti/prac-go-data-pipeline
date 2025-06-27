# prac-go-data-pipeline

## Performance tuning

### 1,000,000 rows
First try: 2m48.625514334s
603k indexed
- 1 transformer
- 1 indexer
    - batch size: 10
    - 60% pass - need to handle partial success
- queue size: 1000
