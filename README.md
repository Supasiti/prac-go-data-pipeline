# prac-go-data-pipeline

## Performance tuning

### 1,000,000 rows
First try: 1m38.552878042s
- 1 transformer
- 1 indexer
    - batch size: 20
- queue size: 1000

Second: 1m38.828214083s
use string builder
- 1 transformer
- 1 indexer
    - batch size: 20
- queue size: 1000

