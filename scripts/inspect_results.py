from pathlib import Path
import sqlite3
db=Path(__file__).resolve().parents[1]/'data'/'pipeline.db'
with sqlite3.connect(db) as c:
    for table in ['pipeline_runs','data_quality_metrics','customer_spend_eur','country_category_revenue']:
        print(f'\n{table}')
        print(' | '.join(r[1] for r in c.execute(f'PRAGMA table_info({table})')))
        for row in c.execute(f'SELECT * FROM {table} LIMIT 15'): print(' | '.join(map(str,row)))

