# prac-go-data-pipeline

## Performance tuning

### 1,000,000 rows
First try: 1m38.552878042s
- 1 transformer
- 1 indexer
    - batch size: 20
- queue size: 1000

Second: 1m36.440947875s
use string builder
- 1 transformer
- 1 indexer
    - batch size: 20
- queue size: 1000

Third: 1m9.605459084s
- 1 transformer
- 2 indexers
    - batch size: 20
- queue size: 1000
note: one client per indexer have no impact

Fourth: 1m9.176999167s
- 1 transformer
- 4 indexers
    - batch size: 20
- queue size: 1000

transformer : 2.43512925s
- 1 transformer

with reader : 2.423356583s
- 1 transformer
- 2 indexers
    - batch size: 20
- queue size: 1000

