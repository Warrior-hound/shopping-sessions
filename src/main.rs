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
    eprintln!("[{:8.3}s] {} (minflt {})", t, msg, minflt());
}

/// CPU seconds consumed by the calling thread (load-insensitive stage timing).
#[cfg(unix)]
fn tcpu() -> f64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    ts.tv_sec as f64 + ts.tv_nsec as f64 * 1e-9
}
#[cfg(not(unix))]
fn tcpu() -> f64 {
    T0.get().unwrap().elapsed().as_secs_f64()
}
#[cfg(unix)]
fn minflt() -> i64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    ru.ru_minflt as i64
}
#[cfg(not(unix))]
fn minflt() -> i64 {
    0
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

// ------------------------------------------------------------------ hugepage arenas
/// Large anonymous regions, 2 MB aligned, `MADV_HUGEPAGE` (Linux THP under enabled=madvise):
/// ~512x fewer first-touch faults and TLB misses than 4 KB pages, and cheaper teardown.
/// Regions are registered so they can be unmapped in parallel before exit.
const HUGE: usize = 2 << 20;
static REGIONS: std::sync::Mutex<Vec<(usize, usize)>> = std::sync::Mutex::new(Vec::new());

/// Zeroed, 2 MB-aligned region of at least `bytes` (lazily backed; never freed by Drop).
fn huge_region(bytes: usize) -> *mut u8 {
    let len = (bytes.max(1) + HUGE - 1) / HUGE * HUGE;
    #[cfg(unix)]
    unsafe {
        let raw = libc::mmap(
            std::ptr::null_mut(),
            len + HUGE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        if raw == libc::MAP_FAILED {
            panic!("mmap {} bytes failed", len + HUGE);
        }
        let a = (raw as usize + HUGE - 1) / HUGE * HUGE;
        // trim the unaligned head and tail so the region is exactly [a, a + len)
        if a > raw as usize {
            libc::munmap(raw, a - raw as usize);
        }
        let tail = raw as usize + len + HUGE - (a + len);
        if tail > 0 {
            libc::munmap((a + len) as *mut libc::c_void, tail);
        }
        if std::env::var_os("NOHUGE").is_none() {
            libc::madvise(a as *mut libc::c_void, len, libc::MADV_HUGEPAGE);
        }
        REGIONS.lock().unwrap().push((a, len));
        a as *mut u8
    }
    #[cfg(not(unix))]
    unsafe {
        let p = std::alloc::alloc_zeroed(std::alloc::Layout::from_size_align(len, HUGE).unwrap());
        assert!(!p.is_null(), "alloc failed");
        p
    }
}

/// Unmap every registered region (and the given input mappings) in parallel, so exit
/// teardown is not one serial exit_mmap over GBs of pages. Call only after all output is
/// written: nothing may touch these regions afterwards.
fn parallel_teardown(nt: usize, extra: &[(usize, usize)]) {
    #[cfg(unix)]
    {
        let mut v: Vec<(usize, usize)> = std::mem::take(&mut *REGIONS.lock().unwrap());
        v.extend_from_slice(extra);
        // ~64 MB pieces (2 MB multiples) so the threads share the work evenly
        let mut pieces: Vec<(usize, usize)> = Vec::new();
        for (a, l) in v {
            let mut o = 0;
            while o < l {
                let n = (l - o).min(64 << 20);
                pieces.push((a + o, n));
                o += n;
            }
        }
        par(nt, pieces.len(), |_| (), |_, i| unsafe {
            libc::munmap(pieces[i].0 as *mut libc::c_void, pieces[i].1);
        });
    }
    #[cfg(not(unix))]
    {
        let _ = (nt, extra);
    }
}

/// Bump allocator over one huge region (one per worker, single-threaded use).
struct Arena {
    base: *mut u8,
    off: usize,
    cap: usize,
    // consumed BLK_BYTES blocks (already faulted in): reused before fresh arena memory,
    // so later scatter phases write into warm pages instead of zeroing new ones
    free: Vec<*mut u8>,
}
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    fn new(bytes: usize) -> Arena {
        let cap = (bytes.max(1) + HUGE - 1) / HUGE * HUGE;
        Arena { base: huge_region(cap), off: 0, cap, free: Vec::new() }
    }
    #[inline(never)]
    fn block(&mut self) -> *mut u8 {
        let bytes = BLK_BYTES;
        if let Some(p) = self.free.pop() {
            return p;
        }
        if self.off + bytes <= self.cap {
            let p = unsafe { self.base.add(self.off) };
            self.off += bytes;
            p
        } else {
            // arena exhausted (pathological inputs): fall back to a leaked heap block
            let p = unsafe { std::alloc::alloc(std::alloc::Layout::from_size_align(bytes, 64).unwrap()) };
            assert!(!p.is_null(), "alloc failed");
            p
        }
    }
}

/// Append-only bucket made of fixed-size blocks bump-allocated from a worker's arena:
/// dense memory (no per-bucket over-reservation), no realloc copies, no capacity guess.
const BLK_BYTES: usize = 16 << 10;
struct Bkt<T> {
    blocks: Vec<*mut T>,
    cur: *mut T,
    end: *mut T,
    n: usize,
}
unsafe impl<T: Send> Send for Bkt<T> {}
unsafe impl<T: Sync> Sync for Bkt<T> {}

impl<T: Copy> Bkt<T> {
    const PER: usize = BLK_BYTES / std::mem::size_of::<T>();
    fn new() -> Self {
        Bkt { blocks: Vec::new(), cur: std::ptr::null_mut(), end: std::ptr::null_mut(), n: 0 }
    }
    #[inline(always)]
    fn push(&mut self, a: &mut Arena, v: T) {
        if self.cur == self.end {
            self.grow(a);
        }
        unsafe {
            self.cur.write(v);
            self.cur = self.cur.add(1);
        }
        self.n += 1;
    }
    #[inline(never)]
    fn grow(&mut self, a: &mut Arena) {
        let p = a.block() as *mut T;
        self.blocks.push(p);
        self.cur = p;
        self.end = unsafe { p.add(Self::PER) };
    }
    #[inline]
    fn len(&self) -> usize {
        self.n
    }
    /// Hand this bucket's blocks to `a` for reuse. The caller must never read the bucket again.
    fn recycle_into(&self, a: &mut Arena) {
        a.free.extend(self.blocks.iter().map(|&p| p as *mut u8));
    }
    /// The contiguous blocks, in push order.
    #[inline]
    fn slices(&self) -> impl Iterator<Item = &[T]> + '_ {
        let nb = self.blocks.len();
        let last = self.n - nb.saturating_sub(1) * Self::PER;
        self.blocks.iter().enumerate().map(move |(i, &p)| {
            let l = if i + 1 == nb { last } else { Self::PER };
            unsafe { std::slice::from_raw_parts(p as *const T, l) }
        })
    }
    #[inline]
    fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.slices().flat_map(|s| s.iter())
    }
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

// ------------------------------------------------------------------ SIMD mask tokenizer
/// Bitmasks of ',' '"' '\n' over the 128 bytes at p (bit i = byte p+i). Caller guarantees
/// that 128 bytes are readable.
#[inline(always)]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use core::arch::x86_64::*;
        let c = _mm256_set1_epi8(b',' as i8);
        let q = _mm256_set1_epi8(b'"' as i8);
        let n = _mm256_set1_epi8(b'\n' as i8);
        let (mut cm, mut qm, mut nm) = (0u128, 0u128, 0u128);
        for i in 0..4 {
            let v = _mm256_loadu_si256(p.add(32 * i) as *const __m256i);
            cm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32 as u128) << (32 * i);
            qm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, q)) as u32 as u128) << (32 * i);
            nm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, n)) as u32 as u128) << (32 * i);
        }
        (cm, qm, nm)
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        use core::arch::x86_64::*;
        let c = _mm_set1_epi8(b',' as i8);
        let q = _mm_set1_epi8(b'"' as i8);
        let n = _mm_set1_epi8(b'\n' as i8);
        let (mut cm, mut qm, mut nm) = (0u128, 0u128, 0u128);
        for i in 0..8 {
            let v = _mm_loadu_si128(p.add(16 * i) as *const __m128i);
            cm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, c)) as u32 as u128) << (16 * i);
            qm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, q)) as u32 as u128) << (16 * i);
            nm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, n)) as u32 as u128) << (16 * i);
        }
        (cm, qm, nm)
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let (mut cm, mut qm, mut nm) = (0u128, 0u128, 0u128);
        for i in 0..128 {
            let b = *p.add(i);
            cm |= ((b == b',') as u128) << i;
            qm |= ((b == b'"') as u128) << i;
            nm |= ((b == b'\n') as u128) << i;
        }
        (cm, qm, nm)
    }
}

/// Same contract as `for_lines` + `split::<N>`: call f with the N unquoted fields of every
/// valid line starting in [start, end). Lines shorter than 128 bytes (with 128 readable
/// bytes) take the branch-free mask path: field ends come from the comma mask, and each
/// field's quote validity / unquoting is bit arithmetic on the quote mask. Everything else
/// goes through the scalar `split`.
#[inline(always)]
fn for_rows<const N: usize>(data: S, start: usize, end: usize, mut f: impl FnMut(&[S; N])) {
    let len = data.len();
    let base = data.as_ptr();
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
    let mut out: [S; N] = [b""; N];
    while p < end && p < len {
        if p + 128 <= len {
            let (cm, qm, nm) = unsafe { masks128(base.add(p)) };
            if nm != 0 {
                let e = nm.trailing_zeros() as usize; // line is [p, p + e), e < 128
                let lm = (1u128 << e) - 1;
                let cm = cm & lm;
                if cm.count_ones() as usize == N - 1 {
                    let qm = qm & lm;
                    let mut ok = true;
                    let mut s = 0usize;
                    let mut c = cm;
                    for k in 0..N {
                        let fe = if k + 1 < N {
                            let t = c.trailing_zeros() as usize;
                            c &= c - 1;
                            t
                        } else {
                            e
                        };
                        let l = fe - s; // < 128
                        let q = (qm >> s) & ((1u128 << l) - 1);
                        let want = 1u128 | 1u128.wrapping_shl((l as u32).wrapping_sub(1));
                        let isq = q != 0;
                        // valid: no quote, or exactly '"' at both ends of a field of length >= 2
                        ok &= !isq | ((q == want) & (l >= 2));
                        let a = s + isq as usize;
                        let b = (fe - isq as usize).max(a);
                        out[k] = unsafe { std::slice::from_raw_parts(base.add(p + a), b - a) };
                        s = fe + 1;
                    }
                    if ok {
                        f(&out);
                    }
                }
                p += e + 1;
                continue;
            }
        }
        let e = match memchr::memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => len,
        };
        if split(&data[p..e], &mut out) {
            f(&out);
        }
        p = e + 1;
    }
}

/// Same as `idk` (kept for call sites that know the mapping end; `lim` is unused).
#[inline(always)]
fn idk16(s: S, _lim: usize) -> K {
    idk(s)
}

/// ingest_ts -> ms. R-TS.ING: ingest_ts is always valid F1, so a 20-byte value is converted
/// with no digit or calendar checks; any other length keeps the general parser (and its
/// "bad" result), so behaviour is unchanged even if the promise were broken by length.
#[inline(always)]
fn ingest_ms(s: &[u8]) -> i64 {
    if s.len() == 20 && s[19] == b'Z' {
        let d = |i: usize| (s[i].wrapping_sub(b'0')) as i64;
        let y = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
        let mo = d(5) * 10 + d(6);
        let dd = d(8) * 10 + d(9);
        let h = d(11) * 10 + d(12);
        let mi = d(14) * 10 + d(15);
        let se = d(17) * 10 + d(18);
        ((days_from_civil(y, mo, dd) * 24 + h) * 60 + mi) * 60_000 + se * 1000
    } else {
        parse_ts(s).unwrap_or(i64::MAX / 4)
    }
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

// ------------------------------------------------------------------ SWAR timestamps
#[inline(always)]
fn ld8(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}

/// true iff all 8 bytes of x are ASCII digits.
#[inline(always)]
fn dig8(x: u64) -> bool {
    let a = x.wrapping_sub(0x3030_3030_3030_3030);
    let b = x.wrapping_add(0x4646_4646_4646_4646);
    ((a | b | x) & 0x8080_8080_8080_8080) == 0
}

/// 8 ASCII digits (first byte most significant) -> value.
#[inline(always)]
fn val8(x: u64) -> u64 {
    let mut x = x.wrapping_sub(0x3030_3030_3030_3030);
    x = (x.wrapping_mul(10) + (x >> 8)) & 0x00FF_00FF_00FF_00FF;
    x = (x.wrapping_mul(100) + (x >> 16)) & 0x0000_FFFF_0000_FFFF;
    (x.wrapping_mul(10000) + (x >> 32)) & 0xFFFF_FFFF
}

/// `parse_ts` with the same results, faster on the common shapes: F1 and F2 validate digits
/// and separators with 8-byte masks; F3 of 9..=16 digits is two overlapping 8-byte loads.
/// Anything else goes to `parse_ts`.
#[inline(always)]
fn parse_ts_fast(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 || n == 25 {
        // bytes 0..8 "YYYY-MM-", 8..12 "DDTH", 11..19 "HH:MM:SS"
        let a = ld8(s, 0);
        let b = ld8(s, 8);
        let c = ld8(s, 11); // "HH:MM:SS" (bytes 11..19)
        const SA: u64 = 0xFF << 32 | 0xFF << 56;
        const PA: u64 = (b'-' as u64) << 32 | (b'-' as u64) << 56;
        const SB: u64 = 0xFF << 16;
        const PB: u64 = (b'T' as u64) << 16;
        const SC: u64 = 0xFF << 16 | 0xFF << 40;
        const PC: u64 = (b':' as u64) << 16 | (b':' as u64) << 40;
        const Z: u64 = 0x3030_3030_3030_3030;
        let shape = (a & SA) == PA && (b & SB) == PB && (c & SC) == PC;
        let digits = dig8((a & !SA) | (Z & SA)) && dig8((b & !SB & 0xFFFF_FFFF) | (Z & SB) | (Z & !0xFFFF_FFFF)) && dig8((c & !SC) | (Z & SC));
        let is_f1 = n == 20 && s[19] == b'Z';
        let is_f2 = n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':';
        if is_f1 || is_f2 {
            if !(shape && digits) {
                return None;
            }
            let d = |i: usize| (s[i] - b'0') as i64;
            let y = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
            let mo = d(5) * 10 + d(6);
            let dd = d(8) * 10 + d(9);
            let h = d(11) * 10 + d(12);
            let mi = d(14) * 10 + d(15);
            let se = d(17) * 10 + d(18);
            if !(1..=12).contains(&mo) {
                return None;
            }
            let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
            let dim = match mo {
                2 => 28 + leap as i64,
                4 | 6 | 9 | 11 => 30,
                _ => 31,
            };
            if dd < 1 || dd > dim || h > 23 || mi > 59 || se > 59 {
                return None;
            }
            let base = ((days_from_civil(y, mo, dd) * 24 + h) * 60 + mi) * 60_000 + se * 1000;
            if is_f1 {
                return Some(base);
            }
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            return Some(if s[19] == b'+' { base - off } else { base + off });
        }
        return parse_ts(s);
    }
    if (9..=16).contains(&n) {
        let hi = ld8(s, 0);
        let lo = ld8(s, n - 8);
        if !(dig8(hi) && dig8(lo)) {
            return None; // a non-digit in a value that is not F1/F2 shaped: bad (as parse_ts)
        }
        let k = n - 8; // leading digits, 1..=8
        let sh = 8 * (8 - k) as u32;
        let first = (hi << sh) | (Z_LOW[k]);
        return Some((val8(first) * 100_000_000 + val8(lo)) as i64);
    }
    parse_ts(s)
}

/// '0' bytes in the low (8 - k) byte positions (k leading digits shifted up to the top).
const Z_LOW: [u64; 9] = {
    let mut t = [0u64; 9];
    let mut k = 0;
    while k <= 8 {
        t[k] = if k == 8 { 0 } else { 0x3030_3030_3030_3030u64 >> (8 * k) };
        k += 1;
    }
    t
};

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

static INPUTS: std::sync::Mutex<Vec<(usize, usize)>> = std::sync::Mutex::new(Vec::new());

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
    INPUTS.lock().unwrap().push((m.as_ptr() as usize, m.len()));
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
/// Every string ID becomes a u64 key, exactly and injectively:
/// - up to 10 bytes, all from the 64-symbol set `-0-9A-Z_a-z`: each byte as its 6-bit rank in
///   that set (ranks follow byte order), big-endian, padded with rank 0, then the length in the
///   low 4 bits. For two such keys integer order == byte-wise order of the strings (a prefix
///   pads with the smallest symbol and sorts first via the length).
/// - anything else: low nibble 0xF, index into a global interner above it (rare; the generator
///   writes 9-byte lowercase/digit IDs). Ordering of those goes through the bytes (`id_cmp`).
/// The empty ID is key 0. Keys never point into the input.
type K = u64;

const fn rank_table() -> [u8; 256] {
    let mut t = [0xFFu8; 256];
    let mut r = 0u8;
    let mut b = 0usize;
    while b < 256 {
        let c = b as u8;
        if c == b'-' || c == b'_' || c.is_ascii_digit() || c.is_ascii_alphabetic() {
            t[b] = r;
            r += 1;
        }
        b += 1;
    }
    t
}
static RANK: [u8; 256] = rank_table();
const SYM: &[u8; 64] = b"-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

struct LongIds {
    map: hashbrown::HashMap<S, u32, foldhash::fast::FixedState>,
    list: Vec<S>,
}
static LONG: std::sync::Mutex<Option<LongIds>> = std::sync::Mutex::new(None);
static LONG_LIST: std::sync::OnceLock<Vec<S>> = std::sync::OnceLock::new();

#[inline]
fn idk(s: S) -> K {
    let n = s.len();
    if n <= 10 {
        let mut code = 0u64;
        let mut bad = 0u8;
        for &c in s {
            let r = RANK[c as usize];
            bad |= r;
            code = (code << 6) | (r & 63) as u64;
        }
        if bad != 0xFF && bad & 0xC0 == 0 {
            return ((code << (6 * (10 - n))) << 4) | n as u64;
        }
    }
    idk_long(s)
}

#[inline(never)]
fn idk_long(s: S) -> K {
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
    ((i as u64) << 4) | 0xF
}

/// Freeze the long-ID list (call once all parsing is done).
fn freeze_long() {
    let g = LONG.lock().unwrap();
    let v = g.as_ref().map(|l| l.list.clone()).unwrap_or_default();
    let _ = LONG_LIST.set(v);
}

#[inline]
fn is_long(k: K) -> bool {
    (k & 0xF) == 0xF
}

/// Bytes of an ID key. Long keys need `freeze_long` (or the interner) to have run.
fn id_bytes(k: K, out: &mut Vec<u8>) {
    if is_long(k) {
        let i = (k >> 4) as usize;
        match LONG_LIST.get() {
            Some(v) => out.extend_from_slice(v[i]),
            None => out.extend_from_slice(LONG.lock().unwrap().as_ref().unwrap().list[i]),
        }
    } else {
        let n = (k & 0xF) as usize;
        let code = k >> 4;
        for j in 0..n {
            out.push(SYM[((code >> (6 * (9 - j))) & 63) as usize]);
        }
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
    let m = ((k ^ 0x243f_6a88_85a3_08d3) as u128) * 0x9e37_79b9_7f4a_7c15u128;
    let x = (m as u64) ^ ((m >> 64) as u64);
    let m2 = (x as u128) * 0xd6e8_feb8_6659_fd93u128;
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
    tab: &'static [(K, K)], // key 0 (the empty string) marks an empty slot; empty IDs are never linked
    mask: usize,
    n: usize,
}

const PF: usize = 16; // prefetch distance (items)

static LINKS: std::sync::OnceLock<Links> = std::sync::OnceLock::new();
static LK_M: std::sync::Mutex<()> = std::sync::Mutex::new(());
static LK_CV: std::sync::Condvar = std::sync::Condvar::new();

/// The link table, blocking until the link thread has published it.
fn links_wait() -> &'static Links {
    if let Some(l) = LINKS.get() {
        return l;
    }
    let mut g = LK_M.lock().unwrap();
    loop {
        if let Some(l) = LINKS.get() {
            return l;
        }
        g = LK_CV.wait(g).unwrap();
    }
}

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
    let c0 = tcpu();
    let mut pairs: Vec<(K, K)> = Vec::new();
    let mut f: [S; 2] = [b""; 2];
    for &d in lk_files {
        for_lines(d, 0, d.len(), |l| {
            if split(l, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                pairs.push((idk(f[0]), idk(f[1])));
            }
        });
    }
    let c1 = tcpu();
    // One open-addressing table (id key -> value) serves both as the interner (value = id
    // index during union-find) and, rewritten in place, as the final id -> person table.
    // Slots are prefetched PF pairs ahead. Key 0 (the empty ID) marks an empty slot.
    let mut sz = 16usize;
    while sz < pairs.len() * 4 {
        sz *= 2;
    }
    let mask = sz - 1;
    // hugepage-backed: lookups are random over a table far larger than the STLB reach
    let t: &'static mut [(K, K)] = unsafe { std::slice::from_raw_parts_mut(huge_region(sz * std::mem::size_of::<(K, K)>()) as *mut (K, K), sz) };
    let mut ids: Vec<K> = Vec::with_capacity(pairs.len() * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(pairs.len() * 2);
    #[inline(always)]
    fn intern(t: &mut [(K, K)], mask: usize, k: K, ids: &mut Vec<K>, parent: &mut Vec<u32>) -> u32 {
        let mut j = h128(k) as usize & mask;
        loop {
            let e = t[j];
            if e.0 == k {
                return e.1 as u32;
            }
            if e.0 == 0 {
                let i = ids.len() as u32;
                ids.push(k);
                parent.push(i);
                t[j] = (k, i as K);
                return i;
            }
            j = (j + 1) & mask;
        }
    }
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let g = parent[parent[x as usize] as usize];
            parent[x as usize] = g;
            x = g;
        }
        x
    }
    for j in 0..pairs.len() {
        if j + PF < pairs.len() {
            let (a, b) = pairs[j + PF];
            prefetch(&t[h128(a) as usize & mask]);
            prefetch(&t[h128(b) as usize & mask]);
        }
        let (a, b) = pairs[j];
        let ia = intern(t, mask, a, &mut ids, &mut parent);
        let ib = intern(t, mask, b, &mut ids, &mut parent);
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
    // rewrite values in place: id index -> person name (sequential sweep over the table)
    for e in t.iter_mut() {
        if e.0 != 0 {
            let r = find(&mut parent, e.1 as u32);
            e.1 = ids[best[r as usize] as usize];
        }
    }
    log(&format!("links built: {} pairs, cpu parse {:.3} s, union-find + table {:.3} s", pairs.len(), c1 - c0, tcpu() - c1));
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
    if let Some(v) = std::env::var("NP").ok().and_then(|v| v.parse::<usize>().ok()).filter(|v| v.is_power_of_two()) {
        np = v; // knob for A/B only
    }
    let pmask = (np - 1) as u64;
    log(&format!("{} event, {} click, {} link files, {} MB, {} threads, {} partitions", ev_files.len(), cl_files.len(), lk_files.len(), total >> 20, nt, np));

    // ---------------------------------------------------------- links (own thread, overlaps parsing)
    // the link table is published through LINKS; phase 2 starts deduplicating without it
    // and only the person-map steps wait for it
    std::thread::spawn(move || {
        let l = build_links(&lk_files);
        let _ = LINKS.set(l);
        let _g = LK_M.lock().unwrap();
        LK_CV.notify_all();
    });

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
        ar: Arena,
        ev: Vec<Bkt<ERec>>,
        cl: Bkt<CRaw>,
        t_ev: f64,
        t_cl: f64,
    }
    let (cw0, cw1) = (w0 - WEEK, w1);
    // Generous per-(worker, partition) capacities: untouched reserved pages cost nothing,
    // and pre-sizing avoids realloc copies (and their page faults) while scattering.
    let ev_bytes: usize = ev_files.iter().map(|d| d.len()).sum();
    let cl_bytes: usize = cl_files.iter().map(|d| d.len()).sum();
    let p1 = par(
        nt,
        ne + cch.len(),
        |_| P1 { ar: Arena::new(ev_bytes / 40 * std::mem::size_of::<ERec>() + cl_bytes / 30 * std::mem::size_of::<CRaw>() + np * BLK_BYTES), ev: (0..np).map(|_| Bkt::new()).collect(), cl: Bkt::new(), t_ev: 0.0, t_cl: 0.0 },
        |st, t| {
            let tt = tcpu();
            if t < ne {
                let (d, s, e) = ech[t];
                let lim = d.as_ptr() as usize + d.len();
                for_rows::<7>(d, s, e, |f| {
                    if f[0].is_empty() {
                        return;
                    }
                    let ing = ingest_ms(f[3]); // R-TS.ING: always valid F1
                    let mut ts = BAD;
                    let mut price = 0u32;
                    let mut cat = NOCAT;
                    let mut ukey: K = 0;
                    'v: {
                        if f[1].is_empty() || f[5].is_empty() {
                            break 'v;
                        }
                        let t = match parse_ts_fast(f[2]) {
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
                        ukey = idk16(f[1], lim);
                    }
                    let key = idk16(f[0], lim);
                    let h = h128(key);
                    st.ev[((h >> 40) & pmask) as usize].push(&mut st.ar, ERec { key, ukey, ts, ing, price, cat });
                });
                st.t_ev += tcpu() - tt;
            } else {
                let (d, s, e) = cch[t - ne];
                let lim = d.as_ptr() as usize + d.len();
                for_rows::<4>(d, s, e, |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let ts = match parse_ts_fast(f[3]) {
                        Some(t) if t >= cw0 && t < cw1 => t,
                        _ => return,
                    };
                    let camp = match camp_tab.find(hs(f[2]), |&i| camp_ids[i as usize] == f[2]) {
                        Some(&i) => i,
                        None => return,
                    };
                    st.cl.push(&mut st.ar, CRaw { ukey: idk16(f[1], lim), ts, cid: idk16(f[0], lim), camp });
                });
                st.t_cl += tcpu() - tt;
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

    // ---------------------------------------------------------- dedup events, map persons
    // tasks: [0, np) event partitions, then click slices
    const CSLB: usize = 64; // click blocks per task
    let cl_slices: Vec<Vec<&[CRaw]>> = p1.iter().map(|s| s.cl.slices().collect()).collect();
    let mut ctasks: Vec<(usize, usize)> = Vec::new();
    for (w, s) in cl_slices.iter().enumerate() {
        let mut a = 0;
        while a < s.len() {
            ctasks.push((w, a));
            a += CSLB;
        }
    }
    struct P2 {
        ar: Arena,
        ev: Vec<Bkt<PRec>>,
        cl: Vec<Bkt<CRec>>,
        t_dd: f64,
        t_map: f64,
        t_cl: f64,
    }
    let p2 = par(
        nt,
        np + ctasks.len(),
        |_| P2 { ar: Arena::new(ev_bytes / 40 * std::mem::size_of::<PRec>() + cl_bytes / 30 * std::mem::size_of::<CRec>() + 2 * np * BLK_BYTES), ev: (0..np).map(|_| Bkt::new()).collect(), cl: (0..np).map(|_| Bkt::new()).collect(), t_dd: 0.0, t_map: 0.0, t_cl: 0.0 },
        |st, p| {
            let tt = tcpu();
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
                // bucket p of every worker is consumed: reuse its blocks for this worker's pushes
                for s in p1.iter() {
                    s.ev[p].recycle_into(&mut st.ar);
                }
                let t1 = tcpu();
                st.t_dd += t1 - tt;
                let links = links_wait();
                // person lookups straight off the table (no survivor copy); a second iterator
                // runs PF survivors ahead and prefetches their link-table slots
                let mut ahead = tab.iter().filter(|r| r.ts != BAD);
                for _ in 0..PF {
                    if let Some(r) = ahead.next() {
                        links.prefetch(r.ukey);
                    }
                }
                for r in tab.iter().filter(|r| r.ts != BAD) {
                    if let Some(a) = ahead.next() {
                        links.prefetch(a.ukey);
                    }
                    let pk = links.person(r.ukey);
                    st.ev[((h128(pk) >> 40) & pmask) as usize].push(&mut st.ar, PRec { pk, ts: r.ts, price: r.price, cat: r.cat });
                }
                drop(tab);
                st.t_map += tcpu() - t1;
            } else {
                let (w, a) = ctasks[p - np];
                let links = links_wait();
                let v = &cl_slices[w];
                for sl in &v[a..(a + CSLB).min(v.len())] {
                    for (j, r) in sl.iter().enumerate() {
                        if j + PF < sl.len() {
                            links.prefetch(sl[j + PF].ukey);
                        }
                        let pk = links.person(r.ukey);
                        st.cl[((h128(pk) >> 40) & pmask) as usize].push(&mut st.ar, CRec { pk, ts: r.ts, cid: r.cid, camp: r.camp });
                    }
                    // this click block is consumed (each block belongs to exactly one task)
                    st.ar.free.push(sl.as_ptr() as *mut u8);
                }
                st.t_cl += tcpu() - tt;
            }
        },
    );
    {
        let n: usize = p2.iter().map(|s| s.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
        let a: f64 = p2.iter().map(|s| s.t_dd).sum();
        let b: f64 = p2.iter().map(|s| s.t_map).sum();
        let c: f64 = p2.iter().map(|s| s.t_cl).sum();
        log(&format!("dedup: {} surviving events (thread-s: dedup {:.2}, event person map {:.2}, click person map {:.2})", n, a, b, c));
        log(&format!("links: {} ids", links_wait().n));
    }

    freeze_long();

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
            let tt = tcpu();
            let cap: usize = p2.iter().map(|s| s.ev[q].len()).sum();
            let mut tab: HashTable<u32> = HashTable::with_capacity(cap / 8 + 16);
            let mut names: Vec<K> = Vec::new();
            let mut cnt: Vec<u32> = Vec::new();
            let mut pids: Vec<u32> = Vec::with_capacity(cap);
            for s in p2.iter() {
                for r in s.ev[q].iter() {
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
                for r in s.ev[q].iter() {
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
                for r in s.cl[q].iter() {
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
                for r in s.cl[q].iter() {
                    let pid = cp[k];
                    k += 1;
                    if pid != u32::MAX {
                        cks[cpos[pid as usize]] = Ck { ts: r.ts, cid: r.cid, camp: r.camp };
                        cpos[pid as usize] += 1;
                    }
                }
            }
            drop(cp);
            let t1 = tcpu();
            st.t_grp += t1 - tt;
            for pid in 0..npers {
                evs[off[pid]..off[pid + 1]].sort_unstable_by_key(|x| x.ts);
                cks[coff[pid]..coff[pid + 1]].sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| id_cmp(a.cid, b.cid)));
            }
            let t2 = tcpu();
            st.t_sort += t2 - t1;

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
            st.t_sess += tcpu() - t2;
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
    if std::env::var_os("DIAG").is_some() {
        if let Ok(st) = std::fs::read_to_string("/proc/self/smaps_rollup") {
            for l in st.lines().filter(|l| l.starts_with("AnonHugePages") || l.starts_with("Rss")) {
                log(l);
            }
        }
    }
    // All output is written and closed. Unmap the arenas and input mappings in parallel
    // (exit_mmap would do it on one CPU), then exit without running destructors.
    let tear: u32 = std::env::var("TEAR").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
    if tear >= 1 {
        parallel_teardown(nt, &[]);
        log("teardown arenas");
    }
    if tear >= 2 {
        let inputs = std::mem::take(&mut *INPUTS.lock().unwrap());
        parallel_teardown(nt, &inputs);
        log("teardown");
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    /// Differential test: for_rows (mask tokenizer) == for_lines + split on adversarial CSV.
    fn check<const N: usize>(seed: u64) {
        let mut x = seed;
        let mut rnd = |m: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % m
        };
        let alpha = b"ab,\"\"\n,x\"9";
        let mut v: Vec<u8> = b"h\n".to_vec();
        for _ in 0..20000 {
            let kind = rnd(10);
            let len = if kind == 0 { rnd(300) } else { rnd(40) };
            for _ in 0..len {
                v.push(if rnd(3) == 0 { alpha[rnd(alpha.len() as u64) as usize] } else { b'a' + rnd(5) as u8 });
            }
            // well-formed-ish rows too
            if kind < 5 {
                for k in 0..N {
                    if k > 0 {
                        v.push(b',');
                    }
                    let q = rnd(4) == 0;
                    if q {
                        v.push(b'"');
                    }
                    for _ in 0..rnd(20) {
                        v.push(b'0' + rnd(10) as u8);
                    }
                    if q && rnd(8) != 0 {
                        v.push(b'"');
                    }
                }
            }
            if rnd(50) != 0 {
                v.push(b'\n');
            }
        }
        let data: S = Box::leak(v.into_boxed_slice());
        for ch in [1usize, 7, 64, 129, 4096, data.len()] {
            let mut a: Vec<Vec<Vec<u8>>> = Vec::new();
            let mut b: Vec<Vec<Vec<u8>>> = Vec::new();
            let mut s = 0;
            while s < data.len() {
                let e = (s + ch).min(data.len());
                let mut f: [S; N] = [b""; N];
                for_lines(data, s, e, |l| {
                    if split(l, &mut f) {
                        a.push(f.iter().map(|x| x.to_vec()).collect());
                    }
                });
                for_rows::<N>(data, s, e, |f| b.push(f.iter().map(|x| x.to_vec()).collect()));
                s = e;
            }
            assert_eq!(a.len(), b.len());
            assert!(a == b, "mismatch seed {} ch {}", seed, ch);
        }
    }
    #[test]
    fn tokenizer_matches_scalar() {
        for seed in 1..40u64 {
            check::<7>(seed * 7919);
            check::<4>(seed * 104729);
            check::<2>(seed * 31);
        }
    }
    #[test]
    fn key_order_and_roundtrip() {
        let alpha: &[u8] = b"-09AZ_az\t\xc3 .~";
        let mut x = 12345u64;
        let mut ids: Vec<Vec<u8>> = vec![Vec::new()];
        for _ in 0..20000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let n = (x % 13) as usize;
            let v: Vec<u8> = (0..n).map(|j| alpha[((x >> (5 * j % 60)) % alpha.len() as u64) as usize]).collect();
            ids.push(v);
        }
        let ks: Vec<K> = ids.iter().map(|v| idk(Box::leak(v.clone().into_boxed_slice()))).collect();
        freeze_long();
        for i in 0..ids.len() {
            let mut b = Vec::new();
            id_bytes(ks[i], &mut b);
            assert_eq!(b, ids[i]);
            let j = (i * 7919) % ids.len();
            assert_eq!(id_cmp(ks[i], ks[j]), ids[i].cmp(&ids[j]), "{:?} {:?}", ids[i], ids[j]);
            assert_eq!(ks[i] == ks[j], ids[i] == ids[j]);
        }
        assert_eq!(idk(b""), 0);
    }
    #[test]
    fn parse_ts_fast_matches() {
        let seeds: [&[u8]; 12] = [b"2027-12-09T00:00:00Z", b"2028-02-29T23:59:59Z", b"2027-02-29T10:00:00Z", b"2026-10-15T09:08:07+05:30",
            b"2026-10-15T09:08:07-23:59", b"1829168499000", b"0001791071890000", b"86400000", b"2027-12-09T24:00:00Z", b"2027-13-09T00:00:00Z",
            b"123456789", b"1234567890123456"];
        let alpha = b"0123456789-T:Z+zt. e";
        let mut x = 99u64;
        let mut n = 0;
        for _ in 0..400000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let mut v = seeds[(x % 12) as usize].to_vec();
            let m = (x >> 8) % 4;
            for j in 0..m {
                let pos = ((x >> (12 + 7 * j)) as usize) % (v.len() + 1);
                let c = alpha[((x >> (40 + 3 * j)) % alpha.len() as u64) as usize];
                match (x >> (20 + j)) % 3 {
                    0 if pos < v.len() => v[pos] = c,
                    1 => v.insert(pos, c),
                    _ if pos < v.len() => {
                        v.remove(pos);
                    }
                    _ => {}
                }
            }
            if (x >> 50) % 5 == 0 {
                let d = (x % 10000) as u8;
                for (i, b) in v.iter_mut().enumerate() {
                    if b.is_ascii_digit() {
                        *b = b'0' + ((d as usize + i * 7) % 10) as u8;
                    }
                }
            }
            assert_eq!(parse_ts_fast(&v), parse_ts(&v), "{:?}", String::from_utf8_lossy(&v));
            n += parse_ts(&v).is_some() as u32;
        }
        assert!(n > 10000);
        for &sd in &seeds {
            assert_eq!(parse_ts_fast(sd), parse_ts(sd));
        }
    }
    #[test]
    fn idk16_matches_idk() {
        let buf: S = b"abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz";
        let lim = buf.as_ptr() as usize + buf.len();
        for a in 0..buf.len() {
            for b in a..buf.len().min(a + 15) {
                assert_eq!(idk16(&buf[a..b], lim), idk(&buf[a..b]));
            }
        }
    }
    #[test]
    fn ingest_matches_parse_ts() {
        for s in ["2027-12-09T00:00:00Z", "2028-02-29T23:59:59Z", "1999-12-31T12:34:56Z", "2026-10-15T09:08:07Z"] {
            assert_eq!(ingest_ms(s.as_bytes()), parse_ts(s.as_bytes()).unwrap());
        }
    }
}
