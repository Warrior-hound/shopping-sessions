//! Shopping Sessions: native pipeline implementing data/shopping-sessions/kit/rules.md.
//!
//! Stages (each timed to stderr):
//!   0. mmap every input file (leaked: every string is a &'static [u8] into the mmap)
//!   1. products / campaigns (small, sequential); links parsed in parallel, union-find,
//!      person name = byte-wise smallest ID of the component
//!   2. events + clicks parsed in parallel byte-range chunks (only '\n' ends a line).
//!      Each event copy is fully validated on its own (validity only depends on the copy),
//!      and bucketed by hash(event_id). Clicks are validated, mapped to their person and
//!      bucketed by hash(person).
//!   3. per event_id bucket: dedup keeping the smallest ingest_ts; survivors mapped to their
//!      person and re-bucketed by hash(person)
//!   4. per person bucket: group, sort, sessionize, credit (latest click <= start within 7d,
//!      ties -> smallest click_id), assists (islands of [click, click+7d] per campaign counted
//!      against sorted session starts), aggregate
//!   5. Parquet write (one user_daily file per worker, one campaign_daily file)
use hashbrown::hash_table::{Entry, HashTable};
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 604_800_000;
const BAD: i64 = i64::MIN;
const NOCAT: u32 = u32::MAX;

type S = &'static [u8];

#[inline]
fn hs(s: &[u8]) -> u64 {
    foldhash::fast::FixedState::with_seed(0x9e37_79b9_7f4a_7c15).hash_one(s)
}

static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
fn log(msg: &str) {
    let t = T0.get().unwrap().elapsed().as_secs_f64();
    eprintln!("[{:8.3}s] {}", t, msg);
}

// ------------------------------------------------------------------ parallel helper
fn par<St: Send>(nt: usize, ntasks: usize, init: impl Fn(usize) -> St + Sync, f: impl Fn(&mut St, usize) + Sync) -> Vec<St> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|w| {
                let (next, init, f) = (&next, &init, &f);
                s.spawn(move || {
                    let mut st = init(w);
                    loop {
                        let t = next.fetch_add(1, Ordering::Relaxed);
                        if t >= ntasks {
                            break;
                        }
                        f(&mut st, t);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ------------------------------------------------------------------ CSV dialect
/// Split a line (no '\n') into exactly N fields. A field holding a quote must be
/// '"' + quote-free + '"' (quotes stripped). Wrong count / bad quoting -> false.
#[inline]
fn split<const N: usize>(line: S, out: &mut [S; N]) -> bool {
    let len = line.len();
    let mut nf = 0usize;
    let mut i = 0usize;
    loop {
        let mut j = i;
        let mut q = 0u32;
        while j < len {
            let c = line[j];
            if c == b',' {
                break;
            }
            q += (c == b'"') as u32;
            j += 1;
        }
        if nf >= N {
            return false;
        }
        let f = &line[i..j];
        out[nf] = if q == 0 {
            f
        } else if q == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
            &f[1..f.len() - 1]
        } else {
            return false;
        };
        nf += 1;
        if j == len {
            break;
        }
        i = j + 1;
    }
    nf == N
}

/// Call f on every line whose start offset lies in [start, end); the file's first line
/// (header) is skipped. Only '\n' ends a line; a final line without '\n' still counts.
#[inline]
fn for_lines(data: S, start: usize, end: usize, mut f: impl FnMut(S)) {
    let len = data.len();
    let mut p = if start == 0 {
        match memchr::memchr(b'\n', data) {
            Some(i) => i + 1,
            None => return,
        }
    } else {
        match memchr::memchr(b'\n', &data[start - 1..]) {
            Some(i) => start + i,
            None => return,
        }
    };
    while p < end && p < len {
        let e = match memchr::memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => len,
        };
        f(&data[p..e]);
        p = e + 1;
    }
}

#[inline]
fn all_digits(s: &[u8]) -> bool {
    !s.is_empty() && s.iter().all(|c| c.is_ascii_digit())
}

// ------------------------------------------------------------------ timestamps
#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some((a as i64) * 10 + b as i64)
    } else {
        None
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn datetime(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d < 1 || d > dim || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}

/// F1 / F2 / F3 -> epoch ms; None when bad. F3 values too long for i64 saturate
/// (they are only outside every window).
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n == 20 && s[19] == b'Z' {
        return datetime(&s[..19]);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let base = datetime(&s[..19])?;
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if s[19] == b'+' { base - off } else { base + off });
    }
    if !all_digits(s) {
        return None;
    }
    let mut v: i64 = 0;
    let mut nd = 0;
    for &c in s {
        if nd > 0 || c != b'0' {
            nd += 1;
            if nd > 18 {
                return Some(i64::MAX / 4);
            }
            v = v * 10 + (c - b'0') as i64;
        }
    }
    Some(v)
}

// ------------------------------------------------------------------ input files
fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<S> {
    let f = std::fs::File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let m = unsafe { memmap2::MmapOptions::new().map(&f) }.ok()?;
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::Sequential);
    }
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    Some(&m[..])
}

fn chunks(files: &[S], ch: usize) -> Vec<(S, usize, usize)> {
    let mut out = Vec::new();
    for &d in files {
        let mut s = 0;
        while s < d.len() {
            let e = (s + ch).min(d.len());
            out.push((d, s, e));
            s = e;
        }
    }
    out
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let k = format!("\"{}\"", key);
    let i = text.find(&k).expect("manifest key") + k.len();
    let rest = &text[i..];
    let c = rest.find(':').unwrap();
    let rest = &rest[c + 1..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    parse_ts(rest[..q2].as_bytes()).expect("manifest ts")
}

// ------------------------------------------------------------------ IDs as integer keys
/// Every string ID becomes a u128 key, exactly and injectively:
/// - <= 15 bytes: the bytes big-endian, zero padded, length in the low byte. For two such
///   keys, integer order == byte-wise order of the strings (a prefix sorts first via length).
/// - longer: low byte 0xFF, index into a global interner above it (rare; generator never
///   writes one). Ordering of long keys goes through the bytes (`id_cmp`).
/// Keys never point into the input, so dedup / grouping / sorting never chase pointers
/// into the 2 GB mmap.
type K = u128;

struct LongIds {
    map: hashbrown::HashMap<S, u32, foldhash::fast::FixedState>,
    list: Vec<S>,
}
static LONG: std::sync::Mutex<Option<LongIds>> = std::sync::Mutex::new(None);
static LONG_LIST: std::sync::OnceLock<Vec<S>> = std::sync::OnceLock::new();

#[inline]
fn idk(s: S) -> K {
    if s.len() <= 15 {
        let mut b = [0u8; 16];
        b[..s.len()].copy_from_slice(s);
        b[15] = s.len() as u8;
        return u128::from_be_bytes(b);
    }
    let mut g = LONG.lock().unwrap();
    let l = g.get_or_insert_with(|| LongIds { map: Default::default(), list: Vec::new() });
    let i = match l.map.get(s) {
        Some(&i) => i,
        None => {
            let i = l.list.len() as u32;
            l.list.push(s);
            l.map.insert(s, i);
            i
        }
    };
    ((i as u128) << 8) | 0xFF
}

/// Freeze the long-ID list (call once all parsing is done).
fn freeze_long() {
    let g = LONG.lock().unwrap();
    let v = g.as_ref().map(|l| l.list.clone()).unwrap_or_default();
    let _ = LONG_LIST.set(v);
}

#[inline]
fn is_long(k: K) -> bool {
    (k as u8) == 0xFF
}

/// Bytes of an ID key. Long keys need `freeze_long` (or the interner) to have run.
fn id_bytes(k: K, out: &mut Vec<u8>) {
    if is_long(k) {
        let i = (k >> 8) as usize;
        match LONG_LIST.get() {
            Some(v) => out.extend_from_slice(v[i]),
            None => out.extend_from_slice(LONG.lock().unwrap().as_ref().unwrap().list[i]),
        }
    } else {
        let b = k.to_be_bytes();
        out.extend_from_slice(&b[..b[15] as usize]);
    }
}

#[inline]
fn id_cmp(a: K, b: K) -> std::cmp::Ordering {
    if !is_long(a) && !is_long(b) {
        return a.cmp(&b);
    }
    let (mut x, mut y) = (Vec::new(), Vec::new());
    id_bytes(a, &mut x);
    id_bytes(b, &mut y);
    x.cmp(&y)
}

#[inline]
fn h128(k: K) -> u64 {
    let lo = (k as u64) ^ 0x243f_6a88_85a3_08d3;
    let hi = ((k >> 64) as u64) ^ 0x1319_8a2e_0370_7344;
    let m = (lo as u128) * (hi as u128);
    let x = (m as u64) ^ ((m >> 64) as u64);
    let m2 = (x as u128) * 0x9e37_79b9_7f4a_7c15u128;
    (m2 as u64) ^ ((m2 >> 64) as u64)
}

// ------------------------------------------------------------------ records
/// One parsed event copy, bucketed by hash(event_id).
#[derive(Clone, Copy)]
struct ERec {
    key: K,
    ukey: K,
    ts: i64, // BAD when this copy does not survive validation + late check
    ing: i64,
    price: u32,
    cat: u32, // NOCAT for view/cart
}

#[derive(Clone, Copy)]
struct CRaw {
    ukey: K,
    ts: i64,
    cid: K,
    camp: u32,
}

/// Surviving event, bucketed by hash(person).
#[derive(Clone, Copy)]
struct PRec {
    pk: K,
    ts: i64,
    price: u32,
    cat: u32,
}

/// Valid click, bucketed by hash(person).
#[derive(Clone, Copy)]
struct CRec {
    pk: K,
    ts: i64,
    cid: K,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Ev {
    ts: i64,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy)]
struct Ck {
    ts: i64,
    cid: K,
    camp: u32,
}

/// Linked IDs -> person key: open addressing (linear probing) over (id key, person key).
struct Links {
    tab: Vec<(K, K)>, // key 0 (the empty string) marks an empty slot; empty IDs are never linked
    mask: usize,
    n: usize,
}

const PF: usize = 16; // prefetch distance (items)

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
}

impl Links {
    #[inline(always)]
    fn prefetch(&self, k: K) {
        if self.n != 0 {
            prefetch(&self.tab[h128(k) as usize & self.mask]);
        }
    }

    #[inline]
    fn person(&self, k: K) -> K {
        if self.n == 0 {
            return k;
        }
        let mut i = h128(k) as usize & self.mask;
        loop {
            let e = &self.tab[i];
            if e.0 == k {
                return e.1;
            }
            if e.0 == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn build_links(lk_files: &[S]) -> Links {
    let mut pairs: Vec<(K, K)> = Vec::new();
    let mut f: [S; 2] = [b""; 2];
    for &d in lk_files {
        for_lines(d, 0, d.len(), |l| {
            if split(l, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                pairs.push((idk(f[0]), idk(f[1])));
            }
        });
    }
    let mut tab: HashTable<u32> = HashTable::with_capacity(pairs.len());
    let mut ids: Vec<K> = Vec::with_capacity(pairs.len());
    let mut parent: Vec<u32> = Vec::with_capacity(pairs.len());
    let mut intern = |k: K, ids: &mut Vec<K>, parent: &mut Vec<u32>| -> u32 {
        match tab.entry(h128(k), |&i| ids[i as usize] == k, |&i| h128(ids[i as usize])) {
            Entry::Occupied(o) => *o.get(),
            Entry::Vacant(v) => {
                let i = ids.len() as u32;
                ids.push(k);
                parent.push(i);
                v.insert(i);
                i
            }
        }
    };
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let g = parent[parent[x as usize] as usize];
            parent[x as usize] = g;
            x = g;
        }
        x
    }
    for &(a, b) in &pairs {
        let ia = intern(a, &mut ids, &mut parent);
        let ib = intern(b, &mut ids, &mut parent);
        let ra = find(&mut parent, ia);
        let rb = find(&mut parent, ib);
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    let n = ids.len();
    let mut best: Vec<u32> = (0..n as u32).collect(); // per root: index of the byte-wise smallest id
    for i in 0..n as u32 {
        let r = find(&mut parent, i) as usize;
        if id_cmp(ids[i as usize], ids[best[r] as usize]).is_lt() {
            best[r] = i;
        }
    }
    let mut sz = 16usize;
    while sz < n * 2 {
        sz *= 2;
    }
    let mask = sz - 1;
    let mut t = vec![(0u128, 0u128); sz];
    for i in 0..n {
        let name = ids[best[parent[i] as usize] as usize];
        let mut j = h128(ids[i]) as usize & mask;
        while t[j].0 != 0 {
            j = (j + 1) & mask;
        }
        t[j] = (ids[i], name);
    }
    Links { tab: t, mask, n }
}

#[derive(Default)]
struct URows {
    name: Vec<K>,
    day: Vec<i32>,
    ses: Vec<i64>,
    ev: Vec<i64>,
    rev: Vec<i64>,
    top: Vec<u32>,
}

fn flush(rows: &mut URows, name: K, day: i64, ses: i64, ev: i64, rev: i64, catsum: &[(u32, i64)]) {
    let mut top = NOCAT;
    let mut best = -1i64;
    for &(c, v) in catsum {
        if v > best || (v == best && c < top) {
            best = v;
            top = c;
        }
    }
    rows.name.push(name);
    rows.day.push(day as i32);
    rows.ses.push(ses);
    rows.ev.push(ev);
    rows.rev.push(rev);
    rows.top.push(top);
}

fn main() {
    T0.set(Instant::now()).ok();
    let args: Vec<String> = std::env::args().collect();
    let (data, out) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into())),
        )
    };
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
        .max(1);

    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let w0 = manifest_ts(&man, "window_start");
    let w1 = manifest_ts(&man, "window_end");
    let day0 = w0.div_euclid(DAY);
    let ndays = ((w1 - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    // ---------------------------------------------------------- mmap
    let ev_files: Vec<S> = csv_files(&data.join("events")).iter().filter_map(|p| map_file(p)).collect();
    let cl_files: Vec<S> = csv_files(&data.join("clicks")).iter().filter_map(|p| map_file(p)).collect();
    let lk_files: Vec<S> = csv_files(&data.join("links")).iter().filter_map(|p| map_file(p)).collect();
    let total: usize = ev_files.iter().chain(cl_files.iter()).map(|d| d.len()).sum();
    let mut np = 64usize;
    while np < 8192 && np * (6 << 20) < total {
        np *= 2;
    }
    let pmask = (np - 1) as u64;
    log(&format!("{} event, {} click, {} link files, {} MB, {} threads, {} partitions", ev_files.len(), cl_files.len(), lk_files.len(), total >> 20, nt, np));

    // ---------------------------------------------------------- links (own thread, overlaps parsing)
    let lk_thread = std::thread::spawn(move || build_links(&lk_files));

    // ---------------------------------------------------------- products, campaigns
    let mut prod: hashbrown::HashMap<S, S, foldhash::fast::FixedState> = Default::default();
    if let Some(d) = map_file(&data.join("products.csv")) {
        let mut f: [S; 2] = [b""; 2];
        for_lines(d, 0, d.len(), |l| {
            if split(l, &mut f) && !f[0].is_empty() {
                prod.insert(f[0], f[1]);
            }
        });
    }
    let mut cats: Vec<S> = prod.values().copied().collect();
    cats.push(b"unknown");
    cats.sort_unstable();
    cats.dedup();
    let unknown_rank = cats.binary_search(&&b"unknown"[..]).unwrap() as u32;
    let mut prod_tab: HashTable<(S, u32)> = HashTable::with_capacity(prod.len());
    for (&p, &c) in prod.iter() {
        let r = cats.binary_search(&c).unwrap() as u32;
        prod_tab.insert_unique(hs(p), (p, r), |e| hs(e.0));
    }
    let mut camp_ids: Vec<S> = Vec::new();
    let mut camp_ch: Vec<S> = Vec::new();
    let mut camp_tab: HashTable<u32> = HashTable::new();
    if let Some(d) = map_file(&data.join("campaigns.csv")) {
        let mut f: [S; 2] = [b""; 2];
        for_lines(d, 0, d.len(), |l| {
            if split(l, &mut f) && !f[0].is_empty() {
                let h = hs(f[0]);
                if camp_tab.find(h, |&i| camp_ids[i as usize] == f[0]).is_none() {
                    let i = camp_ids.len() as u32;
                    camp_ids.push(f[0]);
                    camp_ch.push(f[1]);
                    camp_tab.insert_unique(h, i, |&j| hs(camp_ids[j as usize]));
                }
            }
        });
    }
    let ncamp = camp_ids.len();
    log(&format!("products {} ({} cats), campaigns {}", prod.len(), cats.len(), ncamp));

    // ---------------------------------------------------------- parse events + clicks
    let ech = chunks(&ev_files, 4 << 20);
    let cch = chunks(&cl_files, 4 << 20);
    let ne = ech.len();
    struct P1 {
        ev: Vec<Vec<ERec>>,
        cl: Vec<CRaw>,
        t_ev: f64,
        t_cl: f64,
    }
    let (cw0, cw1) = (w0 - WEEK, w1);
    // Generous per-(worker, partition) capacities: untouched reserved pages cost nothing,
    // and pre-sizing avoids realloc copies (and their page faults) while scattering.
    let ev_bytes: usize = ev_files.iter().map(|d| d.len()).sum();
    let cl_bytes: usize = cl_files.iter().map(|d| d.len()).sum();
    let est_ev = (ev_bytes / 60 / np / nt) * 2 + 64;
    let est_cl = (cl_bytes / 30 / np / nt) * 2 + 64;
    let p1 = par(
        nt,
        ne + cch.len(),
        |_| P1 { ev: (0..np).map(|_| Vec::with_capacity(est_ev)).collect(), cl: Vec::with_capacity(cl_bytes / 30 / nt * 2 + 64), t_ev: 0.0, t_cl: 0.0 },
        |st, t| {
            let tt = Instant::now();
            if t < ne {
                let (d, s, e) = ech[t];
                let mut f: [S; 7] = [b""; 7];
                for_lines(d, s, e, |l| {
                    if !split(l, &mut f) || f[0].is_empty() {
                        return;
                    }
                    let ing = parse_ts(f[3]).unwrap_or(i64::MAX / 4);
                    let mut ts = BAD;
                    let mut price = 0u32;
                    let mut cat = NOCAT;
                    let mut ukey: K = 0;
                    'v: {
                        if f[1].is_empty() || f[5].is_empty() {
                            break 'v;
                        }
                        let t = match parse_ts(f[2]) {
                            Some(t) if t >= w0 && t < w1 => t,
                            _ => break 'v,
                        };
                        let et = f[4];
                        if et == b"purchase" {
                            let p = f[6];
                            if !all_digits(p) {
                                break 'v;
                            }
                            let mut v: u64 = 0;
                            for &c in p {
                                v = v * 10 + (c - b'0') as u64;
                                if v > 10_000_000 {
                                    break 'v;
                                }
                            }
                            if v < 1 {
                                break 'v;
                            }
                            price = v as u32;
                            let ph = hs(f[5]);
                            cat = match prod_tab.find(ph, |e| e.0 == f[5]) {
                                Some(e) => e.1,
                                None => unknown_rank,
                            };
                        } else if et != b"view" && et != b"cart" {
                            break 'v;
                        }
                        if ing - t > DAY {
                            break 'v;
                        }
                        ts = t;
                        ukey = idk(f[1]);
                    }
                    let key = idk(f[0]);
                    let h = h128(key);
                    st.ev[((h >> 40) & pmask) as usize].push(ERec { key, ukey, ts, ing, price, cat });
                });
                st.t_ev += tt.elapsed().as_secs_f64();
            } else {
                let (d, s, e) = cch[t - ne];
                let mut f: [S; 4] = [b""; 4];
                for_lines(d, s, e, |l| {
                    if !split(l, &mut f) || f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let ts = match parse_ts(f[3]) {
                        Some(t) if t >= cw0 && t < cw1 => t,
                        _ => return,
                    };
                    let camp = match camp_tab.find(hs(f[2]), |&i| camp_ids[i as usize] == f[2]) {
                        Some(&i) => i,
                        None => return,
                    };
                    st.cl.push(CRaw { ukey: idk(f[1]), ts, cid: idk(f[0]), camp });
                });
                st.t_cl += tt.elapsed().as_secs_f64();
            }
        },
    );
    {
        let ne: usize = p1.iter().map(|s| s.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
        let nc: usize = p1.iter().map(|s| s.cl.len()).sum();
        let te: f64 = p1.iter().map(|s| s.t_ev).sum();
        let tc: f64 = p1.iter().map(|s| s.t_cl).sum();
        log(&format!("parsed: {} event copies ({:.2} thread-s), {} valid clicks ({:.2} thread-s)", ne, te, nc, tc));
    }
    let links = lk_thread.join().unwrap();
    freeze_long();
    log(&format!("links: {} ids", links.n));

    // ---------------------------------------------------------- dedup events, map persons
    // tasks: [0, np) event partitions, then click slices
    const CSL: usize = 1 << 16;
    let mut ctasks: Vec<(usize, usize)> = Vec::new();
    for (w, s) in p1.iter().enumerate() {
        let mut a = 0;
        while a < s.cl.len() {
            ctasks.push((w, a));
            a += CSL;
        }
    }
    struct P2 {
        ev: Vec<Vec<PRec>>,
        cl: Vec<Vec<CRec>>,
        t_dd: f64,
        t_map: f64,
        t_cl: f64,
    }
    let p2 = par(
        nt,
        np + ctasks.len(),
        |_| P2 { ev: (0..np).map(|_| Vec::with_capacity(est_ev)).collect(), cl: (0..np).map(|_| Vec::with_capacity(est_cl)).collect(), t_dd: 0.0, t_map: 0.0, t_cl: 0.0 },
        |st, p| {
            let tt = Instant::now();
            if p < np {
                let cap: usize = p1.iter().map(|s| s.ev[p].len()).sum();
                // event_id -> (ingest of the kept copy, worker, index); min ingest wins
                // event_id -> kept copy (smallest ingest_ts). The table holds the records
                // themselves, so reading the winners back is one sequential sweep.
                let mut tab: HashTable<ERec> = HashTable::with_capacity(cap);
                for s in p1.iter() {
                    for r in s.ev[p].iter() {
                        match tab.entry(h128(r.key), |e| e.key == r.key, |e| h128(e.key)) {
                            Entry::Occupied(mut o) => {
                                let e = o.get_mut();
                                if r.ing < e.ing {
                                    *e = *r;
                                }
                            }
                            Entry::Vacant(v) => {
                                v.insert(*r);
                            }
                        }
                    }
                }
                let t1 = Instant::now();
                st.t_dd += (t1 - tt).as_secs_f64();
                let sv: Vec<(K, i64, u32, u32)> = tab.iter().filter(|r| r.ts != BAD).map(|r| (r.ukey, r.ts, r.price, r.cat)).collect();
                drop(tab);
                // person lookups; link-table slots prefetched ahead (the table is far larger than cache)
                for j in 0..sv.len() {
                    if j + PF < sv.len() {
                        links.prefetch(sv[j + PF].0);
                    }
                    let (uk, ts, price, cat) = sv[j];
                    let pk = links.person(uk);
                    st.ev[((h128(pk) >> 40) & pmask) as usize].push(PRec { pk, ts, price, cat });
                }
                st.t_map += t1.elapsed().as_secs_f64();
            } else {
                let (w, a) = ctasks[p - np];
                let v = &p1[w].cl;
                let sl = &v[a..(a + CSL).min(v.len())];
                for (j, r) in sl.iter().enumerate() {
                    if j + PF < sl.len() {
                        links.prefetch(sl[j + PF].ukey);
                    }
                    let pk = links.person(r.ukey);
                    st.cl[((h128(pk) >> 40) & pmask) as usize].push(CRec { pk, ts: r.ts, cid: r.cid, camp: r.camp });
                }
                st.t_cl += tt.elapsed().as_secs_f64();
            }
        },
    );
    {
        let n: usize = p2.iter().map(|s| s.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
        let a: f64 = p2.iter().map(|s| s.t_dd).sum();
        let b: f64 = p2.iter().map(|s| s.t_map).sum();
        let c: f64 = p2.iter().map(|s| s.t_cl).sum();
        log(&format!("dedup: {} surviving events (thread-s: dedup {:.2}, event person map {:.2}, click person map {:.2})", n, a, b, c));
    }

    // ---------------------------------------------------------- per person
    struct P3 {
        rows: URows,
        acc: Vec<[i64; 4]>, // per (campaign incl. organic, day): sessions, purchasing, revenue, assisted
        t_grp: f64,
        t_sort: f64,
        t_sess: f64,
    }
    let organic = ncamp as u32;
    let p3 = par(
        nt,
        np,
        |_| P3 { rows: URows::default(), acc: vec![[0i64; 4]; (ncamp + 1) * ndays], t_grp: 0.0, t_sort: 0.0, t_sess: 0.0 },
        |st, q| {
            let tt = Instant::now();
            let cap: usize = p2.iter().map(|s| s.ev[q].len()).sum();
            let mut tab: HashTable<u32> = HashTable::with_capacity(cap / 8 + 16);
            let mut names: Vec<K> = Vec::new();
            let mut cnt: Vec<u32> = Vec::new();
            let mut pids: Vec<u32> = Vec::with_capacity(cap);
            for s in p2.iter() {
                for r in &s.ev[q] {
                    let pid = match tab.entry(h128(r.pk), |&i| names[i as usize] == r.pk, |&i| h128(names[i as usize])) {
                        Entry::Occupied(o) => *o.get(),
                        Entry::Vacant(v) => {
                            let i = names.len() as u32;
                            names.push(r.pk);
                            cnt.push(0);
                            v.insert(i);
                            i
                        }
                    };
                    cnt[pid as usize] += 1;
                    pids.push(pid);
                }
            }
            let npers = names.len();
            if npers == 0 {
                return;
            }
            let mut off = vec![0usize; npers + 1];
            for i in 0..npers {
                off[i + 1] = off[i] + cnt[i] as usize;
            }
            let mut pos = off.clone();
            let mut evs: Vec<Ev> = vec![Ev { ts: 0, price: 0, cat: 0 }; cap];
            let mut k = 0;
            for s in p2.iter() {
                for r in &s.ev[q] {
                    let pid = pids[k] as usize;
                    k += 1;
                    evs[pos[pid]] = Ev { ts: r.ts, price: r.price, cat: r.cat };
                    pos[pid] += 1;
                }
            }
            drop(pids);
            // clicks of persons with events
            let mut ccnt = vec![0u32; npers + 1];
            let mut cp: Vec<u32> = Vec::new();
            for s in p2.iter() {
                for r in &s.cl[q] {
                    let pid = match tab.find(h128(r.pk), |&i| names[i as usize] == r.pk) {
                        Some(&i) => i,
                        None => u32::MAX,
                    };
                    if pid != u32::MAX {
                        ccnt[pid as usize] += 1;
                    }
                    cp.push(pid);
                }
            }
            let mut coff = vec![0usize; npers + 1];
            for i in 0..npers {
                coff[i + 1] = coff[i] + ccnt[i] as usize;
            }
            let mut cpos = coff.clone();
            let mut cks: Vec<Ck> = vec![Ck { ts: 0, cid: 0, camp: 0 }; coff[npers]];
            let mut k = 0;
            for s in p2.iter() {
                for r in &s.cl[q] {
                    let pid = cp[k];
                    k += 1;
                    if pid != u32::MAX {
                        cks[cpos[pid as usize]] = Ck { ts: r.ts, cid: r.cid, camp: r.camp };
                        cpos[pid as usize] += 1;
                    }
                }
            }
            drop(cp);
            let t1 = Instant::now();
            st.t_grp += (t1 - tt).as_secs_f64();
            for pid in 0..npers {
                evs[off[pid]..off[pid + 1]].sort_unstable_by_key(|x| x.ts);
                cks[coff[pid]..coff[pid + 1]].sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| id_cmp(a.cid, b.cid)));
            }
            let t2 = Instant::now();
            st.t_sort += (t2 - t1).as_secs_f64();

            let mut starts: Vec<i64> = Vec::new();
            let mut catsum: Vec<(u32, i64)> = Vec::new();
            let mut cc: Vec<(u32, i64)> = Vec::new();
            for pid in 0..npers {
                let e = &evs[off[pid]..off[pid + 1]];
                let c = &cks[coff[pid]..coff[pid + 1]];
                let name = names[pid];
                starts.clear();
                // user_daily accumulators for the current day
                let mut cur_day = i64::MIN;
                let (mut d_ses, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
                catsum.clear();
                let mut hi = 0usize;
                let mut i = 0usize;
                while i < e.len() {
                    let start = e[i].ts;
                    let mut j = i;
                    let mut rev = 0i64;
                    let mut purch = false;
                    let mut prev = start;
                    let day = start.div_euclid(DAY);
                    if day != cur_day {
                        if cur_day != i64::MIN {
                            flush(&mut st.rows, name, cur_day, d_ses, d_ev, d_rev, &catsum);
                        }
                        cur_day = day;
                        d_ses = 0;
                        d_ev = 0;
                        d_rev = 0;
                        catsum.clear();
                    }
                    // equal timestamps have gap 0, so ties always share a session (order-free)
                    while j < e.len() && e[j].ts - prev <= GAP {
                        let x = e[j];
                        prev = x.ts;
                        if x.cat != NOCAT {
                            purch = true;
                            rev += x.price as i64;
                            match catsum.iter_mut().find(|c| c.0 == x.cat) {
                                Some(c) => c.1 += x.price as i64,
                                None => catsum.push((x.cat, x.price as i64)),
                            }
                        }
                        j += 1;
                    }
                    d_ses += 1;
                    d_ev += (j - i) as i64;
                    d_rev += rev;
                    starts.push(start);
                    // credit: latest click with ts <= start (clicks sorted by ts, then click_id)
                    while hi < c.len() && c[hi].ts <= start {
                        hi += 1;
                    }
                    let mut camp = organic;
                    if hi > 0 {
                        let t = c[hi - 1].ts;
                        if start - t <= WEEK {
                            let mut k = hi - 1;
                            while k > 0 && c[k - 1].ts == t {
                                k -= 1;
                            }
                            camp = c[k].camp; // smallest click_id among the latest ts
                        }
                    }
                    let a = &mut st.acc[camp as usize * ndays + (day - day0) as usize];
                    a[0] += 1;
                    a[1] += purch as i64;
                    a[2] += rev;
                    i = j;
                }
                if cur_day != i64::MIN {
                    flush(&mut st.rows, name, cur_day, d_ses, d_ev, d_rev, &catsum);
                }
                // assists: per campaign, merge [click, click + 7d] into islands and count the
                // session starts inside each island, per day (binary search on sorted starts)
                if !c.is_empty() {
                    cc.clear();
                    cc.extend(c.iter().map(|x| (x.camp, x.ts)));
                    cc.sort_unstable();
                    let mut k = 0;
                    while k < cc.len() {
                        let camp = cc[k].0;
                        let a = cc[k].1;
                        let mut b = a;
                        k += 1;
                        while k < cc.len() && cc[k].0 == camp && cc[k].1 - b <= WEEK {
                            b = cc[k].1;
                            k += 1;
                        }
                        let lo = starts.partition_point(|&s| s < a);
                        let hi = starts.partition_point(|&s| s <= b + WEEK);
                        let mut x = lo;
                        while x < hi {
                            let d = starts[x].div_euclid(DAY);
                            let nb = (d + 1) * DAY;
                            let y = x + starts[x..hi].partition_point(|&s| s < nb);
                            st.acc[camp as usize * ndays + (d - day0) as usize][3] += (y - x) as i64;
                            x = y;
                        }
                    }
                }
            }
            st.t_sess += t2.elapsed().as_secs_f64();
        },
    );
    {
        let a: f64 = p3.iter().map(|s| s.t_grp).sum();
        let b: f64 = p3.iter().map(|s| s.t_sort).sum();
        let c: f64 = p3.iter().map(|s| s.t_sess).sum();
        log(&format!("sessionized + attributed (thread-s: group {:.2}, sort {:.2}, sessionize/credit/assist {:.2})", a, b, c));
    }

    // ---------------------------------------------------------- write
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir");
    std::fs::create_dir_all(&cd).expect("mkdir");
    let mut acc = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for s in &p3 {
        for (a, b) in acc.iter_mut().zip(s.acc.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    let nonempty: Vec<usize> = (0..p3.len()).filter(|&i| !p3[i].rows.name.is_empty()).collect();
    let cats = &cats;
    let nrows: usize = p3.iter().map(|s| s.rows.name.len()).sum();
    std::thread::scope(|sc| {
        for (k, &i) in nonempty.iter().enumerate() {
            let r = &p3[i].rows;
            let ud = &ud;
            sc.spawn(move || write_user(&ud.join(format!("part-{:05}.parquet", k)), r, cats));
        }
        if nonempty.is_empty() {
            write_user(&ud.join("part-00000.parquet"), &URows::default(), cats);
        }
        let mut rows: Vec<(S, S, i32, [i64; 4])> = Vec::new();
        for c in 0..=ncamp {
            for d in 0..ndays {
                let a = acc[c * ndays + d];
                if a[0] > 0 || a[3] > 0 {
                    let (id, ch): (S, S) = if c == ncamp { (b"organic", b"organic") } else { (camp_ids[c], camp_ch[c]) };
                    rows.push((id, ch, (day0 + d as i64) as i32, a));
                }
            }
        }
        write_campaign(&cd.join("part-00000.parquet"), &rows);
        log(&format!("campaign_daily {} rows", rows.len()));
    });
    log(&format!("done: user_daily {} rows", nrows));
    if let Ok(st) = std::fs::read_to_string("/proc/self/status") {
        if let Some(l) = st.lines().find(|l| l.starts_with("VmHWM")) {
            log(l);
        }
    }
    // Skip freeing GBs of buckets/mappings: the kernel reclaims them at exit (RESEARCH card 6).
    std::process::exit(0);
}

// ------------------------------------------------------------------ parquet
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .build(),
    )
}

fn bas(v: &[S]) -> Vec<ByteArray> {
    v.iter().map(|&s| ByteArray::from(bytes::Bytes::from_static(s))).collect()
}

fn write_user(path: &Path, r: &URows, cats: &[S]) {
    let schema = Arc::new(
        parse_message_type(
            "message schema { REQUIRED BYTE_ARRAY user_id (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; \
             REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BYTE_ARRAY top_category (UTF8); }",
        )
        .unwrap(),
    );
    let f = std::fs::File::create(path).expect("create");
    let mut w = SerializedFileWriter::new(std::io::BufWriter::with_capacity(1 << 20, f), schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut c = rg.next_column().unwrap().unwrap();
    // names: one shared buffer, zero-copy slices per row
    let mut buf: Vec<u8> = Vec::with_capacity(r.name.len() * 10);
    let mut ends: Vec<usize> = Vec::with_capacity(r.name.len());
    for &k in &r.name {
        id_bytes(k, &mut buf);
        ends.push(buf.len());
    }
    let buf = bytes::Bytes::from(buf);
    let mut st = 0;
    let names: Vec<ByteArray> = ends
        .iter()
        .map(|&e| {
            let b = ByteArray::from(buf.slice(st..e));
            st = e;
            b
        })
        .collect();
    c.typed::<ByteArrayType>().write_batch(&names, None, None).unwrap();
    c.close().unwrap();
    let mut c = rg.next_column().unwrap().unwrap();
    c.typed::<Int32Type>().write_batch(&r.day, None, None).unwrap();
    c.close().unwrap();
    for v in [&r.ses, &r.ev, &r.rev] {
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<Int64Type>().write_batch(v, None, None).unwrap();
        c.close().unwrap();
    }
    let mut c = rg.next_column().unwrap().unwrap();
    let defs: Vec<i16> = r.top.iter().map(|&t| (t != NOCAT) as i16).collect();
    let vals: Vec<S> = r.top.iter().filter(|&&t| t != NOCAT).map(|&t| cats[t as usize]).collect();
    c.typed::<ByteArrayType>().write_batch(&bas(&vals), Some(&defs), None).unwrap();
    c.close().unwrap();
    rg.close().unwrap();
    w.close().unwrap();
}

fn write_campaign(path: &Path, rows: &[(S, S, i32, [i64; 4])]) {
    let schema = Arc::new(
        parse_message_type(
            "message schema { REQUIRED BYTE_ARRAY campaign_id (UTF8); REQUIRED BYTE_ARRAY channel (UTF8); REQUIRED INT32 day (DATE); \
             REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }",
        )
        .unwrap(),
    );
    let f = std::fs::File::create(path).expect("create");
    let mut w = SerializedFileWriter::new(std::io::BufWriter::new(f), schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let ids: Vec<S> = rows.iter().map(|r| r.0).collect();
    let chs: Vec<S> = rows.iter().map(|r| r.1).collect();
    let days: Vec<i32> = rows.iter().map(|r| r.2).collect();
    for v in [&ids, &chs] {
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<ByteArrayType>().write_batch(&bas(v), None, None).unwrap();
        c.close().unwrap();
    }
    let mut c = rg.next_column().unwrap().unwrap();
    c.typed::<Int32Type>().write_batch(&days, None, None).unwrap();
    c.close().unwrap();
    for k in 0..4 {
        let v: Vec<i64> = rows.iter().map(|r| r.3[k]).collect();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
    }
    rg.close().unwrap();
    w.close().unwrap();
}
