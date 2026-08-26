from __future__ import annotations
import hashlib, json, logging, os, sqlite3, sys, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql"
DEFAULT_URL = "https://jzozteoirwfczccltcdr.supabase.co/rest/v1/orders_raw?apikey=sb_publishable_Xwjiw--qkKcbMuSbKd6I2w_wN9mpNTv"
LOG = logging.getLogger("etl")

def now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")

def get_json(url, attempts=3):
    for attempt in range(attempts):
        try:
            with urlopen(Request(url, headers={"Accept":"application/json","User-Agent":"aqurate-etl/1.0"}), timeout=30) as response:
                return json.loads(response.read().decode())
        except Exception:
            if attempt == attempts-1: raise
            time.sleep(2**attempt)

def connect(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn=sqlite3.connect(path); conn.row_factory=sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON"); conn.execute("PRAGMA journal_mode=WAL")
    return conn

def sql(conn, name): conn.executescript((SQL_DIR/name).read_text(encoding="utf-8"))

def ingest(conn, url):
    rows=get_json(url)
    if not isinstance(rows,list) or not rows: raise ValueError("Orders endpoint returned no rows")
    expected={'order_id','customer_id','customer_email','order_ts','status','channel','sku','product_name','category','qty','unit_price','currency','country','fx_reference_date'}
    if expected-set(rows[0]): raise ValueError(f"Missing columns: {sorted(expected-set(rows[0]))}")
    conn.execute("DELETE FROM orders_raw")
    columns=['order_id','customer_id','customer_email','order_ts','status','channel','sku','product_name','category','qty','unit_price','currency','country','fx_reference_date']
    records=[]
    for row in rows:
        raw=json.dumps(row,sort_keys=True,separators=(',',':'))
        records.append((hashlib.sha256(raw.encode()).hexdigest(),now(),*(row.get(c) for c in columns),raw))
    conn.executemany("INSERT INTO orders_raw(row_hash,ingested_at,"+','.join(columns)+",raw_json) VALUES ("+','.join('?' for _ in range(17))+")",records)
    return len(rows)

def fetch_fx(conn):
    dates=[r[0] for r in conn.execute("SELECT DISTINCT fx_reference_date FROM orders_clean WHERE currency='RON' AND fx_reference_date<=date('now')")]
    for day in dates:
        payload=get_json(f"https://api.frankfurter.app/{day}?from=RON&to=EUR"); rate=payload.get('rates',{}).get('EUR')
        if not rate or rate<=0: raise ValueError(f"Bad FX response for {day}")
        conn.execute("INSERT INTO fx_rates VALUES(?,?,'RON','EUR',?,?) ON CONFLICT(requested_date,base_currency,quote_currency) DO UPDATE SET rate_date=excluded.rate_date,rate=excluded.rate,fetched_at=excluded.fetched_at",(day,payload['date'],rate,now()))
    return len(dates)

def run(db_path, url=DEFAULT_URL):
    conn=connect(Path(db_path)); sql(conn,'01_schema.sql')
    run_id=conn.execute("INSERT INTO pipeline_runs(started_at,status) VALUES(?,'running')",(now(),)).lastrowid; conn.commit()
    try:
        raw=ingest(conn,url); sql(conn,'02_clean.sql'); fx=fetch_fx(conn); sql(conn,'03_marts.sql')
        clean=conn.execute("SELECT count(*) FROM orders_clean").fetchone()[0]
        metrics={'raw_rows':raw,'clean_rows':clean,'rejected_rows':conn.execute("SELECT count(*) FROM orders_rejected").fetchone()[0],
          'missing_fx_orders':conn.execute("SELECT count(*) FROM orders_clean o LEFT JOIN fx_rates f ON f.requested_date=o.fx_reference_date AND f.base_currency=o.currency WHERE o.currency<>'EUR' AND f.rate IS NULL").fetchone()[0]}
        conn.executemany("INSERT INTO data_quality_metrics VALUES(?,?,?)",[(run_id,k,v) for k,v in metrics.items()])
        conn.execute("UPDATE pipeline_runs SET finished_at=?,status='success',raw_rows=?,clean_rows=?,fx_rates_loaded=? WHERE run_id=?",(now(),raw,clean,fx,run_id)); conn.commit()
        LOG.info("success raw=%d clean=%d fx=%d",raw,clean,fx)
    except Exception as exc:
        conn.rollback(); conn.execute("UPDATE pipeline_runs SET finished_at=?,status='failed',error_message=? WHERE run_id=?",(now(),str(exc)[:1000],run_id)); conn.commit(); raise
    finally: conn.close()

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    try: run(Path(os.getenv('DATABASE_PATH',ROOT/'data'/'pipeline.db')),os.getenv('ORDERS_API_URL',DEFAULT_URL))
    except Exception: LOG.exception('pipeline failed'); sys.exit(1)

