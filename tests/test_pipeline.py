from src.pipeline import connect, sql

def insert(conn,**change):
    row=dict(row_hash='unique',ingested_at='now',order_id='ORD1',customer_id=42,customer_email='customer42@example.com',order_ts='1781474381',status='completed',channel='web',sku='SKUEL001',product_name='Earbuds',category=None,qty=2,unit_price=10,currency='EUR',country='RO',fx_reference_date='2026-08-26',raw_json='{}'); row.update(change)
    conn.execute(f"INSERT INTO orders_raw({','.join(row)}) VALUES({','.join('?' for _ in row)})",tuple(row.values()))

def test_cleaning(tmp_path):
    c=connect(tmp_path/'t.db'); sql(c,'01_schema.sql'); insert(c,customer_id=None); sql(c,'02_clean.sql'); r=c.execute('SELECT * FROM orders_clean').fetchone()
    assert (r['customer_id'],r['sku'],r['category'])==(42,'SKU-EL-001','Electronics')
    c.close()

def test_quarantine(tmp_path):
    c=connect(tmp_path/'t.db'); sql(c,'01_schema.sql'); insert(c,qty=-1); sql(c,'02_clean.sql')
    assert c.execute('SELECT count(*) FROM orders_clean').fetchone()[0]==0
    assert c.execute('SELECT rejection_reason FROM orders_rejected').fetchone()[0]=='non_positive_quantity'
    c.close()

def test_line_items_not_order_duplicates(tmp_path):
    c=connect(tmp_path/'t.db'); sql(c,'01_schema.sql'); insert(c,row_hash='a'); insert(c,row_hash='a'); insert(c,row_hash='b',sku='SKU-BK-001',category='Books'); sql(c,'02_clean.sql')
    assert c.execute('SELECT count(*) FROM orders_clean').fetchone()[0]==2
    c.close()
