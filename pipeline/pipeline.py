#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB + NumPy pipeline implementing every rule in rules.md.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)

Stages:
  1. lines: every CSV read as whole raw lines (only '\n' ends a line; '\r' kept).
  2. parse: per-line regex validates field count + quoting (R-CSV.5), then unquote.
  3. links -> connected components (scipy, C-level) -> person = byte-wise min ID.
  4. events: dedup on event_id (min ingest_ts) -> validate -> late -> sessionize.
  5. clicks: validate; credit via ASOF join (ties reduced to min click_id first).
  6. assists: per (person, campaign) union of [click, click+7d] islands, counted
     against sorted session starts with searchsorted (no range join).
"""
import glob
import json
import mmap
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

T0 = time.time()
DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
if len(sys.argv) >= 3:  # run_fixtures.py passes DATA OUT as the last two args
    DATA, OUT = sys.argv[-2], sys.argv[-1]
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

DAY = 86_400_000
GAP = 1_800_000
WEEK = 604_800_000


def log(msg):
    print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr, flush=True)


con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.7))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")


def q(s):
    return "'" + s.replace("'", "''") + "'"


# ---------------------------------------------------------------- timestamps
# A ts string -> epoch ms (BIGINT) or NULL when bad (R-TS). Digits are ASCII only.
con.execute(r"""
CREATE MACRO days_civil(y, m, d) AS (
    -- Howard Hinnant's days_from_civil, shifted by 400 years so all ints stay >= 0
    ((y - CASE WHEN m <= 2 THEN 1 ELSE 0 END + 400) // 400) * 146097
    + ((y - CASE WHEN m <= 2 THEN 1 ELSE 0 END + 400) % 400) * 365
    + ((y - CASE WHEN m <= 2 THEN 1 ELSE 0 END + 400) % 400) // 4
    - ((y - CASE WHEN m <= 2 THEN 1 ELSE 0 END + 400) % 400) // 100
    + (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    - 719468 - 146097
)
""")
con.execute(r"""
CREATE MACRO dim(y, m) AS (
    CASE WHEN m IN (1,3,5,7,8,10,12) THEN 31
         WHEN m IN (4,6,9,11) THEN 30
         WHEN (y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0 THEN 29
         ELSE 28 END
)
""")
con.execute(r"""
CREATE MACRO ymdhms_ms(s) AS (
    CASE WHEN TRY_CAST(substr(s,6,2) AS INTEGER) BETWEEN 1 AND 12
          AND TRY_CAST(substr(s,9,2) AS INTEGER) BETWEEN 1 AND dim(TRY_CAST(substr(s,1,4) AS INTEGER), TRY_CAST(substr(s,6,2) AS INTEGER))
          AND TRY_CAST(substr(s,12,2) AS INTEGER) <= 23
          AND TRY_CAST(substr(s,15,2) AS INTEGER) <= 59
          AND TRY_CAST(substr(s,18,2) AS INTEGER) <= 59
    THEN CAST(days_civil(TRY_CAST(substr(s,1,4) AS BIGINT), TRY_CAST(substr(s,6,2) AS BIGINT), TRY_CAST(substr(s,9,2) AS BIGINT)) AS BIGINT) * 86400000
         + TRY_CAST(substr(s,12,2) AS BIGINT) * 3600000
         + TRY_CAST(substr(s,15,2) AS BIGINT) * 60000
         + TRY_CAST(substr(s,18,2) AS BIGINT) * 1000
    END
)
""")
con.execute(r"""
CREATE MACRO parse_ts(s) AS (
    CASE
    WHEN s IS NULL THEN NULL
    WHEN regexp_full_match(s, '[0-9]+') THEN
        CASE WHEN length(ltrim(s, '0')) <= 15 THEN TRY_CAST(CASE WHEN ltrim(s,'0') = '' THEN '0' ELSE ltrim(s,'0') END AS BIGINT) END
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z') THEN ymdhms_ms(s)
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}')
         AND TRY_CAST(substr(s,21,2) AS INTEGER) <= 23 AND TRY_CAST(substr(s,24,2) AS INTEGER) <= 59 THEN
        ymdhms_ms(s) - (CASE WHEN substr(s,20,1) = '+' THEN 1 ELSE -1 END)
                       * (TRY_CAST(substr(s,21,2) AS BIGINT) * 3600000 + TRY_CAST(substr(s,24,2) AS BIGINT) * 60000)
    END
)
""")


def f1_ms(s):
    return con.execute("SELECT parse_ts(?)", [s]).fetchone()[0]


with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as f:
    manifest = json.load(f)
W0 = f1_ms(manifest["window_start"])
W1 = f1_ms(manifest["window_end"])
assert W0 is not None and W1 is not None, manifest
log(f"window {manifest['window_start']} .. {manifest['window_end']}")


# ---------------------------------------------------------------- raw lines
def needs_slow_path(path):
    """DuckDB's CSV reader also ends lines at '\r'; files holding '\r' (or our
    sentinel delimiter \x01) are split exactly by the slow read_text path."""
    with open(path, "rb") as f:
        try:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        except ValueError:  # 0-byte file
            return False
        try:
            return mm.find(b"\r") >= 0 or mm.find(b"\x01") >= 0
        finally:
            mm.close()


def files_of(pattern):
    return sorted(p for p in glob.glob(os.path.join(DATA, pattern)) if os.path.isfile(p) and os.path.getsize(p) > 0)


ALL = {k: files_of(p) for k, p in
       [("events", "events/*.csv"), ("clicks", "clicks/*.csv"), ("links", "links/*.csv"),
        ("products", "products.csv"), ("campaigns", "campaigns.csv")]}
with ThreadPoolExecutor(max(1, NCPU)) as ex:
    flat = [p for v in ALL.values() for p in v]
    slow = dict(zip(flat, ex.map(needs_slow_path, flat)))


def lines_sql(kind):
    """SQL yielding one column `line` with every data line (header skipped)."""
    fast = [p for p in ALL[kind] if not slow[p]]
    slowf = [p for p in ALL[kind] if slow[p]]
    parts = []
    if fast:
        lst = "[" + ",".join(q(p) for p in fast) + "]"
        parts.append(
            f"SELECT line FROM read_csv({lst}, columns={{'line':'VARCHAR'}}, delim=e'\\x01', quote='', escape='', "
            f"header=true, auto_detect=false, strict_mode=false, comment='', null_padding=false)")
    for p in slowf:
        parts.append(
            f"SELECT l.line FROM (SELECT unnest(string_split(content, e'\\n')) AS line, "
            f"unnest(range(len(string_split(content, e'\\n')))) AS i FROM read_text({q(p)})) l WHERE l.i > 0")
    if not parts:
        return "SELECT NULL::VARCHAR AS line WHERE false"
    return " UNION ALL ".join(parts)


def field_re(n):
    fld = '(?:"[^",]*"|[^",]*)'
    return fld + ("," + fld) * (n - 1)


def parsed(kind, names):
    """Valid lines split into unquoted fields; '' -> NULL."""
    n = len(names)
    cols = ", ".join(f"nullif(f[{i + 1}], '') AS {c}" for i, c in enumerate(names))
    return (f"SELECT {cols} FROM (SELECT string_split(replace(line, '\"', ''), ',') AS f FROM ({lines_sql(kind)}) "
            f"WHERE regexp_full_match(line, {q(field_re(n))}))")


# ---------------------------------------------------------------- dimensions
con.execute(f"CREATE TABLE products AS SELECT product_id, any_value(category) AS category FROM ({parsed('products', ['product_id', 'category'])}) "
            f"WHERE product_id IS NOT NULL GROUP BY product_id")
con.execute(f"CREATE TABLE campaigns AS SELECT campaign_id, any_value(channel) AS channel FROM ({parsed('campaigns', ['campaign_id', 'channel'])}) "
            f"WHERE campaign_id IS NOT NULL GROUP BY campaign_id")

# ---------------------------------------------------------------- links -> persons
con.execute(f"CREATE TABLE links AS SELECT a, b FROM ({parsed('links', ['a', 'b'])}) WHERE a IS NOT NULL AND b IS NOT NULL")
con.execute("CREATE TABLE lid AS SELECT id, (row_number() OVER () - 1)::BIGINT AS k FROM (SELECT DISTINCT id FROM (SELECT a AS id FROM links UNION ALL SELECT b FROM links))")
nids = con.execute("SELECT count(*) FROM lid").fetchone()[0]
if nids:
    ab = con.execute("SELECT la.k AS ka, lb.k AS kb FROM links JOIN lid la ON la.id = links.a JOIN lid lb ON lb.id = links.b").fetchnumpy()
    ka, kb = np.asarray(ab["ka"], dtype=np.int64), np.asarray(ab["kb"], dtype=np.int64)
    g = coo_matrix((np.ones(len(ka), dtype=np.int8), (ka, kb)), shape=(nids, nids))
    _, comp = connected_components(g, directed=True, connection="weak")
    import pyarrow as pa
    con.register("lcomp", pa.table({"k": np.arange(nids, dtype=np.int64), "c": comp.astype(np.int64)}))
    con.execute("""CREATE TABLE idmap AS
        WITH x AS (SELECT lid.id, lcomp.c FROM lid JOIN lcomp USING (k)),
             nm AS (SELECT c, min(id) AS person FROM x GROUP BY c)
        SELECT x.id, nm.person FROM x JOIN nm USING (c)""")
else:
    con.execute("CREATE TABLE idmap (id VARCHAR, person VARCHAR)")
log(f"links: {nids} linked ids")

# ---------------------------------------------------------------- events
EV = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
con.execute(f"""CREATE TABLE ev AS
    SELECT event_id, r.user_id AS user_id, r.ts AS ts, r.ingest_ts AS ingest_ts, r.event_type AS event_type,
           r.product_id AS product_id, r.price_cents AS price_cents
    FROM (SELECT event_id, arg_min({{'user_id': user_id, 'ts': ts, 'ingest_ts': ingest_ts, 'event_type': event_type,
                                     'product_id': product_id, 'price_cents': price_cents}}, ingest_ts) AS r
          FROM ({parsed('events', EV)}) WHERE event_id IS NOT NULL GROUP BY event_id)""")
log("events parsed + deduped")
con.execute(f"""CREATE TABLE evv AS
    WITH v AS (
        SELECT user_id, parse_ts(ts) AS t, parse_ts(ingest_ts) AS it, event_type, product_id,
               CASE WHEN event_type = 'purchase' AND regexp_full_match(price_cents, '[0-9]+')
                         AND length(ltrim(price_cents, '0')) BETWEEN 1 AND 8
                    THEN TRY_CAST(ltrim(price_cents, '0') AS BIGINT) END AS price
        FROM ev
        WHERE user_id IS NOT NULL AND product_id IS NOT NULL AND event_type IN ('view', 'cart', 'purchase')
    )
    SELECT coalesce(m.person, v.user_id) AS person, v.t AS ts,
           CASE WHEN v.event_type = 'purchase' THEN v.price ELSE 0 END AS price,
           v.event_type = 'purchase' AS is_purchase,
           coalesce(p.category, 'unknown') AS category
    FROM v LEFT JOIN idmap m ON m.id = v.user_id LEFT JOIN products p ON p.product_id = v.product_id
    WHERE v.t IS NOT NULL AND v.t >= {W0} AND v.t < {W1}
      AND (v.event_type <> 'purchase' OR (v.price BETWEEN 1 AND 10000000))
      AND v.it - v.t <= {DAY}""")
log("events validated")

# ---------------------------------------------------------------- sessionize
con.execute(f"""CREATE TABLE evs AS
    SELECT person, ts, price, is_purchase, category,
           sum(CASE WHEN ts - pts > {GAP} THEN 1 ELSE 0 END) OVER (PARTITION BY person ORDER BY ts ROWS UNBOUNDED PRECEDING) AS sid
    FROM (SELECT *, lag(ts) OVER (PARTITION BY person ORDER BY ts) AS pts FROM evv)""")
con.execute(f"""CREATE TABLE sess AS
    SELECT person, sid, min(ts) AS start, (min(ts) // {DAY}) AS dayn, count(*) AS n_events,
           sum(price) AS revenue, bool_or(is_purchase) AS purchasing
    FROM evs GROUP BY person, sid""")
log("sessionized")

# ---------------------------------------------------------------- user_daily
con.execute("""CREATE TABLE user_daily AS
    WITH sd AS (SELECT s.person, s.sid, s.dayn FROM sess s),
         cat AS (SELECT sd.person, sd.dayn, e.category, sum(e.price) AS amt
                 FROM evs e JOIN sd ON sd.person = e.person AND sd.sid = e.sid
                 WHERE e.is_purchase GROUP BY ALL),
         top AS (SELECT person, dayn, category FROM cat
                 QUALIFY row_number() OVER (PARTITION BY person, dayn ORDER BY amt DESC, category ASC) = 1),
         agg AS (SELECT person, dayn, count(*) AS sessions, sum(n_events) AS events, sum(revenue) AS revenue_cents
                 FROM sess GROUP BY person, dayn)
    SELECT agg.person AS user_id, (DATE '1970-01-01' + CAST(agg.dayn AS INTEGER)) AS day,
           agg.sessions::BIGINT AS sessions, agg.events::BIGINT AS events, agg.revenue_cents::BIGINT AS revenue_cents,
           top.category AS top_category
    FROM agg LEFT JOIN top USING (person, dayn)""")

# ---------------------------------------------------------------- clicks
CL = ["click_id", "user_id", "campaign_id", "ts"]
con.execute(f"""CREATE TABLE clv AS
    WITH c AS (SELECT DISTINCT click_id, user_id, campaign_id, parse_ts(ts) AS t FROM ({parsed('clicks', CL)})
               WHERE click_id IS NOT NULL AND user_id IS NOT NULL)
    SELECT coalesce(m.person, c.user_id) AS person, c.t AS ts, c.click_id, c.campaign_id
    FROM c JOIN campaigns cp ON cp.campaign_id = c.campaign_id LEFT JOIN idmap m ON m.id = c.user_id
    WHERE c.t IS NOT NULL AND c.t >= {W0 - WEEK} AND c.t < {W1}""")
log("clicks validated")

# credit: latest click at/before start within 7d; ts ties -> smallest click_id
con.execute(f"""CREATE TABLE credit AS
    WITH cr AS (SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clv GROUP BY person, ts)
    SELECT s.dayn, s.revenue, s.purchasing,
           CASE WHEN cr.ts IS NOT NULL AND s.start - cr.ts <= {WEEK} THEN cr.campaign_id ELSE 'organic' END AS campaign_id
    FROM sess s ASOF LEFT JOIN cr ON s.person = cr.person AND s.start >= cr.ts""")

# ---------------------------------------------------------------- assists
# Islands: union of [click_ts, click_ts + 7d] per (person, campaign).
con.execute("CREATE TABLE pid AS SELECT person, (row_number() OVER () - 1)::BIGINT AS p FROM (SELECT DISTINCT person FROM sess)")
span = (W1 - (W0 - WEEK)) + 1
base = W0 - WEEK
skeys = con.execute(f"SELECT pid.p * {span} + (s.start - {base}) AS key FROM sess s JOIN pid USING (person) ORDER BY key").fetchnumpy()["key"]
skeys = np.asarray(skeys, dtype=np.int64)
isl = con.execute(f"""
    WITH c AS (SELECT pid.p, clv.campaign_id, clv.ts FROM clv JOIN pid USING (person)),
         g AS (SELECT p, campaign_id, ts,
                      sum(CASE WHEN ts - pts > {WEEK} THEN 1 ELSE 0 END) OVER (PARTITION BY p, campaign_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS gi
               FROM (SELECT *, lag(ts) OVER (PARTITION BY p, campaign_id ORDER BY ts) AS pts FROM c)),
         i AS (SELECT p, campaign_id, greatest(min(ts), {W0}) AS a, least(max(ts) + {WEEK}, {W1 - 1}) AS b
               FROM g GROUP BY p, campaign_id, gi),
         d AS (SELECT p, campaign_id, a, b, unnest(range(a // {DAY}, b // {DAY} + 1)) AS dayn FROM i WHERE a <= b)
    SELECT p, campaign_id, dayn,
           p * {span} + (greatest(a, dayn * {DAY}) - {base}) AS lo,
           p * {span} + (least(b, dayn * {DAY} + {DAY - 1}) - {base}) AS hi
    FROM d""").to_arrow_table()
import pyarrow as pa  # noqa: E402
lo = isl.column("lo").to_numpy()
hi = isl.column("hi").to_numpy()
cnt = np.searchsorted(skeys, hi, side="right") - np.searchsorted(skeys, lo, side="left")
assist_tab = pa.table({"campaign_id": isl.column("campaign_id"), "dayn": isl.column("dayn"),
                       "n": pa.array(cnt.astype(np.int64))})
con.register("assist_raw", assist_tab)
log("assists counted")

con.execute(f"""CREATE TABLE campaign_daily AS
    WITH cr AS (SELECT campaign_id, dayn, count(*) AS sessions, count(*) FILTER (WHERE purchasing) AS purchasing_sessions,
                       sum(revenue) AS revenue_cents FROM credit GROUP BY ALL),
         asr AS (SELECT campaign_id, dayn, sum(n) AS assisted_sessions FROM assist_raw WHERE n > 0 GROUP BY ALL),
         j AS (SELECT coalesce(cr.campaign_id, asr.campaign_id) AS campaign_id, coalesce(cr.dayn, asr.dayn) AS dayn,
                      coalesce(cr.sessions, 0) AS sessions, coalesce(cr.purchasing_sessions, 0) AS purchasing_sessions,
                      coalesce(cr.revenue_cents, 0) AS revenue_cents, coalesce(asr.assisted_sessions, 0) AS assisted_sessions
               FROM cr FULL OUTER JOIN asr ON cr.campaign_id = asr.campaign_id AND cr.dayn = asr.dayn)
    SELECT j.campaign_id, CASE WHEN j.campaign_id = 'organic' THEN 'organic' ELSE cp.channel END AS channel,
           (DATE '1970-01-01' + CAST(j.dayn AS INTEGER)) AS day,
           j.sessions::BIGINT AS sessions, j.purchasing_sessions::BIGINT AS purchasing_sessions,
           j.revenue_cents::BIGINT AS revenue_cents, j.assisted_sessions::BIGINT AS assisted_sessions
    FROM j LEFT JOIN campaigns cp ON cp.campaign_id = j.campaign_id""")

# ---------------------------------------------------------------- write
for t in ("user_daily", "campaign_daily"):
    d = os.path.join(OUT, t)
    os.makedirs(d, exist_ok=True)
    con.execute(f"COPY {t} TO {q(os.path.join(d, 'part-0.parquet'))} (FORMAT parquet)")
log("done: " + ", ".join(f"{t}={con.execute(f'SELECT count(*) FROM {t}').fetchone()[0]}" for t in ("user_daily", "campaign_daily")))
