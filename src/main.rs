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
    // never panic on a closed or broken stderr
    use std::io::Write;
    if TL.load(Ordering::Relaxed) {
        // TIMELINE=1: absolute CLOCK_REALTIME stamp (epoch ms) shared with host-side stamps
        let e = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0);
        let _ = writeln!(std::io::stderr(), "TL {:.3} [{:8.3}s] {} (minflt {})", e, t, msg, minflt());
    } else if DIAGF.load(Ordering::Relaxed) {
        // DIAG=1: resident set size at each log point
        let rss = std::fs::read_to_string("/proc/self/statm").ok().and_then(|v| v.split_whitespace().nth(1).and_then(|x| x.parse::<u64>().ok())).unwrap_or(0) * 4096;
        let _ = writeln!(std::io::stderr(), "[{:8.3}s] {} (minflt {}, rss {} MB)", t, msg, minflt(), rss >> 20);
    } else {
        let _ = writeln!(std::io::stderr(), "[{:8.3}s] {} (minflt {})", t, msg, minflt());
    }
}
static DIAGF: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static TL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// A timeline-only log point (TIMELINE=1); default output unchanged.
fn tlog(msg: &str) {
    if TL.load(Ordering::Relaxed) {
        log(msg);
    }
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
    match try_huge_region(bytes) {
        Some(p) => p,
        None => {
            // address-space limit (RLIMIT_AS, overcommit=2): plain zeroed heap memory
            let len = bytes.max(1);
            let p = unsafe { std::alloc::alloc_zeroed(std::alloc::Layout::from_size_align(len, 64).unwrap()) };
            assert!(!p.is_null(), "out of memory ({} bytes)", len);
            p
        }
    }
}

/// `huge_region`, or None when the reservation is refused.
fn try_huge_region(bytes: usize) -> Option<*mut u8> {
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
            return None;
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
        Some(a as *mut u8)
    }
    #[cfg(not(unix))]
    unsafe {
        let p = std::alloc::alloc_zeroed(std::alloc::Layout::from_size_align(len, HUGE).unwrap());
        if p.is_null() {
            None
        } else {
            Some(p)
        }
    }
}

/// One reservation shared by a phase's workers; each worker takes 2 MB slabs with an atomic
/// bump, so the total reserved is the phase's real need (not workers x worst case). If the
/// reservation is refused, smaller sizes are tried, down to none (all blocks from the heap).
struct Region {
    base: usize,
    cap: usize,
    next: AtomicUsize,
}
const SLAB: usize = HUGE;

impl Region {
    fn new(bytes: usize) -> &'static Region {
        let mut want = (bytes.max(1) + SLAB - 1) / SLAB * SLAB;
        let (mut base, mut cap) = (0usize, 0usize);
        if cfg!(unix) {
            while want >= (64 << 20) {
                if let Some(p) = try_huge_region(want) {
                    base = p as usize;
                    cap = want;
                    break;
                }
                want /= 2;
            }
            if cap == 0 {
                log(&format!("arena reservation of {} bytes refused: heap blocks", bytes));
            }
        }
        Box::leak(Box::new(Region { base, cap, next: AtomicUsize::new(0) }))
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
    reg: &'static Region,
    cur: *mut u8,
    end: *mut u8,
    // consumed BLK_BYTES blocks (already faulted in): reused before fresh arena memory,
    // so later scatter phases write into warm pages instead of zeroing new ones
    free: Vec<*mut u8>,
}
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    fn new(reg: &'static Region) -> Arena {
        Arena { reg, cur: std::ptr::null_mut(), end: std::ptr::null_mut(), free: Vec::new() }
    }
    #[inline(never)]
    fn block(&mut self) -> *mut u8 {
        let bytes = BLK_BYTES;
        if let Some(p) = self.free.pop() {
            return p;
        }
        if (self.end as usize) - (self.cur as usize) < bytes {
            let off = self.reg.next.fetch_add(SLAB, Ordering::Relaxed);
            if off + SLAB <= self.reg.cap {
                self.cur = (self.reg.base + off) as *mut u8;
                self.end = (self.reg.base + off + SLAB) as *mut u8;
            } else {
                // region exhausted or absent: a leaked heap block
                let p = unsafe { std::alloc::alloc(std::alloc::Layout::from_size_align(bytes, 64).unwrap()) };
                assert!(!p.is_null(), "out of memory");
                return p;
            }
        }
        let p = self.cur;
        self.cur = unsafe { p.add(bytes) };
        p
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Line([u8; 64]);

#[inline(always)]
unsafe fn stream64(dst: *mut u8, src: *const u8) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
    {
        use std::arch::x86_64::*;
        let a = _mm256_load_si256(src as *const __m256i);
        let b = _mm256_load_si256(src.add(32) as *const __m256i);
        _mm256_stream_si256(dst as *mut __m256i, a);
        _mm256_stream_si256(dst.add(32) as *mut __m256i, b);
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx")))]
    std::ptr::copy_nonoverlapping(src, dst, 64);
}

#[inline(always)]
fn sfence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_sfence()
    };
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
    // records per block; 12-byte records use 1360 (a multiple of 16: 192-byte write-combining
    // groups never straddle blocks)
    const PER: usize = if std::mem::size_of::<T>() == 12 { 1360 } else { BLK_BYTES / std::mem::size_of::<T>() };
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
    /// Write-combining push (records whose size divides 64): records gather in a cache-line
    /// staging buffer, and each full line goes to the bucket with non-temporal stores (no
    /// read-for-ownership of the destination). Block starts are 64-byte aligned, so lines
    /// never straddle blocks. `len()` counts staged records; `wc_flush` must run (once, at
    /// the end of the phase) before the bucket is read, and the writer must `sfence`.
    #[inline(always)]
    fn push_wc(&mut self, line: &mut Line, a: &mut Arena, v: T) {
        let r = 64 / std::mem::size_of::<T>();
        let k = self.n & (r - 1);
        unsafe { (line.0.as_mut_ptr() as *mut T).add(k).write(v) };
        self.n += 1;
        if k == r - 1 {
            if self.cur == self.end {
                self.grow(a);
            }
            unsafe {
                stream64(self.cur as *mut u8, line.0.as_ptr());
                self.cur = self.cur.add(r);
            }
        }
    }
    /// `push_wc` with the record count kept in a caller-owned compact counter array (hot), so a
    /// push touches only the staging line and the counter; the bucket head (cold) is touched once
    /// per full line. `wc_flush_h` must run before the bucket is read (it sets the count).
    #[inline(always)]
    fn push_wch(&mut self, line: &mut Line, cnt: &mut u32, a: &mut Arena, v: T) {
        let r = 64 / std::mem::size_of::<T>();
        let k = *cnt as usize & (r - 1);
        unsafe { (line.0.as_mut_ptr() as *mut T).add(k).write(v) };
        *cnt += 1;
        if k == r - 1 {
            if self.cur == self.end {
                self.grow(a);
            }
            unsafe {
                stream64(self.cur as *mut u8, line.0.as_ptr());
                self.cur = self.cur.add(r);
            }
        }
    }
    /// `push_wch` for 12-byte records: 16 records gather in three staging lines (192 bytes,
    /// 64-byte aligned in the block since PER is a multiple of 16), streamed when full.
    #[inline(always)]
    fn push_wc12(&mut self, line: &mut [Line; 3], cnt: &mut u32, a: &mut Arena, v: T) {
        debug_assert!(std::mem::size_of::<T>() == 12);
        let k = *cnt as usize & 15;
        unsafe { (line.as_mut_ptr() as *mut T).add(k).write_unaligned(v) };
        *cnt += 1;
        if k == 15 {
            if self.cur == self.end {
                self.grow(a);
            }
            unsafe {
                let d = self.cur as *mut u8;
                let s = line.as_ptr() as *const u8;
                stream64(d, s);
                stream64(d.add(64), s.add(64));
                stream64(d.add(128), s.add(128));
                self.cur = self.cur.add(16);
            }
        }
    }
    fn wc_flush12(&mut self, line: &[Line; 3], cnt: u32, a: &mut Arena) {
        self.n = cnt as usize;
        let k = self.n & 15;
        if k == 0 {
            return;
        }
        if self.cur == self.end {
            self.grow(a);
        }
        unsafe {
            std::ptr::copy_nonoverlapping(line.as_ptr() as *const u8, self.cur as *mut u8, k * 12);
            self.cur = self.cur.add(k);
        }
    }
    fn wc_flush_h(&mut self, line: &Line, cnt: u32, a: &mut Arena) {
        self.n = cnt as usize;
        self.wc_flush(line, a);
    }
    fn wc_flush(&mut self, line: &Line, a: &mut Arena) {
        let r = 64 / std::mem::size_of::<T>();
        let k = self.n & (r - 1);
        if k == 0 {
            return;
        }
        if self.cur == self.end {
            self.grow(a);
        }
        unsafe {
            std::ptr::copy_nonoverlapping(line.0.as_ptr() as *const T, self.cur, k);
            self.cur = self.cur.add(k);
        }
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
#[cfg(test)]
#[inline(always)]
fn for_rows128<const N: usize>(data: S, start: usize, end: usize, mut f: impl FnMut(&[S; N])) {
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
                    // A field is valid iff it has no quote, or '"' exactly at its first and last
                    // byte and length >= 2. Line-wide part: every quote sits on a field's first
                    // byte (after a comma or at 0) or last byte (before a comma or the end).
                    let starts = (cm << 1) | 1;
                    let lasts = (cm | (1u128 << e)) >> 1;
                    let mut ok = (qm & !(starts | lasts)) == 0;
                    // per field: first byte quoted <=> last byte quoted, and not a 1-byte field
                    let qw = [qm as u64, (qm >> 64) as u64];
                    let bit = |i: usize| ((qw[(i >> 6) & 1] >> (i & 63)) & 1) as usize;
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
                        let qs = bit(s) & (l != 0) as usize;
                        let qe = bit(fe.wrapping_sub(1) & 127) & (l != 0) as usize;
                        ok &= (qs == qe) & !((qs == 1) & (l < 2));
                        let a = s + qs;
                        let b = (fe - qs).max(a);
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

/// `idk` via `pack16` (fast path only; the wide path keeps `idk16`).
#[inline(always)]
fn idkf(s: S, lim: usize) -> K {
    match kfast(s, lim) {
        Some(k) => k,
        None if s.is_empty() => 0,
        None => idk_long(s),
    }
}

/// Bitmasks of ',' '"' '\n' over the 64 bytes at p (bit i = byte p+i). 64 bytes readable.
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use core::arch::x86_64::*;
        let c = _mm256_set1_epi8(b',' as i8);
        let q = _mm256_set1_epi8(b'"' as i8);
        let n = _mm256_set1_epi8(b'\n' as i8);
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let mm = |x: __m256i, y: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, y)) as u32 as u64;
        (mm(a, c) | mm(b, c) << 32, mm(a, q) | mm(b, q) << 32, mm(a, n) | mm(b, n) << 32)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut cm, mut qm, mut nm) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let b = *p.add(i);
            cm |= ((b == b',') as u64) << i;
            qm |= ((b == b'"') as u64) << i;
            nm |= ((b == b'\n') as u64) << i;
        }
        (cm, qm, nm)
    }
}

/// Parallel bit extract of a 128-bit value (BMI2 pext on each half); result has popcnt(m) <= 64 bits.
#[inline(always)]
fn pext128(x: u128, m: u128) -> u64 {
    let (xl, xh, ml, mh) = (x as u64, (x >> 64) as u64, m as u64, (m >> 64) as u64);
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    let (l, h) = unsafe { (core::arch::x86_64::_pext_u64(xl, ml), core::arch::x86_64::_pext_u64(xh, mh)) };
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    let (l, h) = {
        let pe = |x: u64, mut m: u64| {
            let (mut r, mut k) = (0u64, 0);
            while m != 0 {
                let b = m & m.wrapping_neg();
                r |= ((x & b != 0) as u64) << k;
                k += 1;
                m ^= b;
            }
            r
        };
        (pe(xl, ml), pe(xh, mh))
    };
    let n = ml.count_ones();
    l | if n >= 64 { 0 } else { h << n }
}

/// Input software-prefetch distance in bytes ahead of the mask stage (0 = off). Knob PFD for A/B;
/// default 512 (paired A/Bs, 8 threads on 4 cores: parse_ev -8%, -10%, -13% vs PFD=0).
static PFD: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(512);

/// Flatten the set bits of m as `off + bit` into out[..], K entries written unconditionally
/// (simdjson stage-1 style), more in a loop when popcnt(m) > K. Returns popcnt(m).
/// out must have room for max(K, popcnt(m)) entries.
#[inline(always)]
unsafe fn flat<const K: usize>(out: *mut u32, mut m: u64, off: u32) -> usize {
    let n = m.count_ones() as usize;
    for i in 0..K {
        *out.add(i) = off.wrapping_add(m.trailing_zeros());
        m &= m.wrapping_sub(1);
    }
    if n > K {
        let mut i = K;
        while m != 0 {
            *out.add(i) = off + m.trailing_zeros();
            m &= m - 1;
            i += 1;
        }
    }
    n
}

/// Same contract as `for_lines` + `split::<N>`: call f with the N unquoted fields of every
/// valid line starting in [start, end). Structural-index tokenizer (simdjson stage 1/2):
/// - Stage 1, per region of up to SEGB + 2 full 64-byte blocks starting at the block that holds
///   the byte before the current line: the ',' '"' '\n' block masks, then three position
///   indexes (u32, relative to the region): every delimiter (',' or '\n'), every '\n', and every
///   "bad" bit. A bad bit marks a quoted-field violation, so that a line is well quoted iff no
///   bad bit lies in [line start, line end]:
///   * a quote that is not exactly one of {right after a delimiter (opener), right before a
///     delimiter (closer)}: a quote inside a field, or a field that is a lone '"';
///   * a delimiter whose field has an opener but no closer, or a closer but no opener (the
///     opener's carry runs through the field's non-delimiter bytes to its delimiter).
///   That is exactly `split`'s rule: a field holding a quote is '"' + quote-free + '"', len >= 2.
/// - Stage 2, per line: its end e is the next '\n' entry; it has N fields iff the delimiter
///   entry N - 1 places on is e; field k is [delim k-1 + 1, delim k) minus its quotes.
/// Lines whose '\n' is not in a fresh region (very long lines, the data end) go to `split`.
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
    let pfd = PFD.load(Ordering::Relaxed);
    const SEGB: usize = 64;
    const NBM: usize = SEGB + 2;
    const SENT: u32 = u32::MAX;
    let mut cmk = [0u64; NBM + 1];
    let mut qmk = [0u64; NBM + 1];
    let mut nmk = [0u64; NBM + 1];
    let mut dix = [SENT; NBM * 64 + 16];
    let mut lix = [SENT; NBM * 64 + 16];
    let mut bix = [SENT; NBM * 64 + 16];
    let (mut dj, mut lj, mut bj) = (0usize, 0usize, 0usize); // cursors; lix[lj] = SENT: no region
    let mut g = 0usize; // region base (absolute)
    let mut seg_at = usize::MAX; // line start the current region was built for
    while p < end && p < len {
        // per-row fields in a fresh local (scalarized into registers: no stores/loads of the
        // 2N words); a single call site for f, so the row closure is inlined into this loop
        let mut out: [S; N] = [b""; N];
        let good = 'row: {
            loop {
                let e = unsafe { *lix.get_unchecked(lj) };
                if e != SENT {
                    let rel = (p - g) as u32;
                    let x = unsafe { *dix.get_unchecked(dj + N - 1) };
                    let bq = unsafe { *bix.get_unchecked(bj) };
                    lj += 1;
                    p = g + e as usize + 1;
                    if (x == e) & (bq > e) {
                        let gp = unsafe { base.add(g) };
                        let mut s = rel;
                        for k in 0..N {
                            let x = if k + 1 < N { unsafe { *dix.get_unchecked(dj + k) } } else { e };
                            let fs = unsafe { (*gp.add(s as usize) == b'"') as u32 };
                            let a = s + fs;
                            out[k] = unsafe { std::slice::from_raw_parts(gp.add(a as usize), (x - fs - a) as usize) };
                            s = x + 1;
                        }
                        dj += N;
                        break 'row true;
                    }
                    unsafe {
                        while *dix.get_unchecked(dj) != e {
                            dj += 1;
                        }
                        dj += 1;
                        while *bix.get_unchecked(bj) <= e {
                            bj += 1;
                        }
                    }
                    break 'row false;
                }
                if seg_at == p {
                    break; // a fresh region does not hold this line's end
                }
                // Stage 1: region [g, g + 64 nb) of full blocks, starting at the byte before p
                seg_at = p;
                g = (p - 1) & !63;
                let nb = ((len - g) >> 6).min(NBM);
                for i in 0..nb {
                    unsafe {
                        (cmk[i], qmk[i], nmk[i]) = masks64(base.add(g + 64 * i));
                    }
                    if pfd != 0 {
                        prefetch(base.wrapping_add(g + 64 * i + pfd));
                    }
                }
                (cmk[nb], qmk[nb], nmk[nb]) = (0, 0, 0);
                let (mut nd, mut nl, mut nbd) = (0usize, 0usize, 0usize);
                let (mut dprev, mut cprev, mut carry) = (0u64, 0u64, 0u64);
                for i in 0..nb {
                    let (cm, qm, nm) = (cmk[i], qmk[i], nmk[i]);
                    let dm = cm | nm;
                    let dn = cmk[i + 1] | nmk[i + 1];
                    let op = ((dm << 1) | dprev) & qm; // quote right after a delimiter
                    let cl = ((dm >> 1) | (dn << 63)) & qm; // quote right before a delimiter
                    let (s1, c1) = (!dm).overflowing_add(op);
                    let (s2, c2) = s1.overflowing_add(carry);
                    carry = (c1 | c2) as u64;
                    let xo = s2 & dm; // delimiters whose field has an opener
                    let xc = ((cl << 1) | cprev) & dm; // delimiters whose field has a closer
                    let mut bad = (qm & !(op ^ cl)) | (xo ^ xc);
                    dprev = dm >> 63;
                    cprev = cl >> 63;
                    let off = (64 * i) as u32;
                    unsafe {
                        nd += flat::<8>(dix.as_mut_ptr().add(nd), dm, off);
                        nl += flat::<2>(lix.as_mut_ptr().add(nl), nm, off);
                        while bad != 0 {
                            *bix.get_unchecked_mut(nbd) = off + bad.trailing_zeros();
                            bad &= bad - 1;
                            nbd += 1;
                        }
                    }
                }
                dix[nd..nd + 16].fill(SENT);
                lix[nl] = SENT;
                bix[nbd] = SENT;
                let rel = (p - g) as u32;
                (dj, lj, bj) = (0, 0, 0);
                while dix[dj] < rel {
                    dj += 1;
                }
                while lix[lj] < rel {
                    lj += 1;
                }
                while bix[bj] < rel {
                    bj += 1;
                }
            }
            // scalar path; the next line builds a fresh region
            lj = 0;
            lix[0] = SENT;
            let e = match memchr::memchr(b'\n', &data[p..]) {
                Some(i) => p + i,
                None => len,
            };
            let mut tmp: [S; N] = [b""; N];
            let ok = split(&data[p..e], &mut tmp);
            out = tmp;
            p = e + 1;
            ok
        };
        if good {
            f(&out);
        }
    }
}

/// The f8-f17 window tokenizer (kept for the differential test).
/// Same contract as `for_lines` + `split::<N>`: call f with the N unquoted fields of every
/// valid line starting in [start, end). simdjson-style stage 1: the ',' '"' '\n' masks of
/// each 64-byte block are computed once and kept in a 3-block (192-byte) window; a line
/// starting at offset o of the first block sees the 128-bit masks of bytes [p, p + 128)
/// through a funnel shift. Lines shorter than 128 bytes take the branch-free mask path:
/// field ends come from the comma mask, and each field's quote validity / unquoting is bit
/// arithmetic on the quote mask. Lines of 128 bytes or more, and lines whose window would
/// pass the end of the data, go through the scalar `split`.
#[cfg(test)]
#[inline(always)]
fn for_rows_win<const N: usize>(data: S, start: usize, end: usize, mut f: impl FnMut(&[S; N])) {
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
    let pfd = PFD.load(Ordering::Relaxed);
    let mut out: [S; N] = [b""; N];
    // Stage 1 (per segment of SEGB blocks, branch-free loop): block masks of [g, g + 64 * nb).
    // Stage 2 (per line): the 3 block masks under [p, p + 128) come from these arrays.
    const SEGB: usize = 64;
    let mut cmk = [0u64; SEGB + 2];
    let mut qmk = [0u64; SEGB + 2];
    let mut nmk = [0u64; SEGB + 2];
    let mut g = usize::MAX; // segment base; lines with b0 in [g, g + 64 * SEGB) use it
    while p < end && p < len {
        let b0 = p & !63;
        // a single call site for f, so the row closure is inlined into this loop
        let good = 'row: {
        if b0 + 192 <= len {
            if b0.wrapping_sub(g) >= SEGB * 64 {
                g = b0;
                let nb = ((len - g) >> 6).min(SEGB + 2);
                for i in 0..nb {
                    unsafe {
                        (cmk[i], qmk[i], nmk[i]) = masks64(base.add(g + 64 * i));
                    }
                    if pfd != 0 {
                        prefetch(base.wrapping_add(g + 64 * i + pfd));
                    }
                }
            }
            let ix = (b0 - g) >> 6; // ix + 2 < nb: block ix + 2 ends at b0 + 192 <= len
            let o = (p & 63) as u32; // 0..63
            let win = |m: &[u64; SEGB + 2]| -> u128 {
                let (x0, x1, x2) = unsafe { (*m.get_unchecked(ix), *m.get_unchecked(ix + 1), *m.get_unchecked(ix + 2)) };
                (((x0 as u128) | ((x1 as u128) << 64)) >> o) | (((x2 as u128) << 64) << (64 - o))
            };
            let nm = win(&nmk);
            let e = nm.trailing_zeros() as usize; // line is [p, p + e); 128 when no '\n'
            if e < 127 {
                let lm = (1u128 << e) - 1;
                let cm = win(&cmk) & lm;
                if cm.count_ones() as usize == N - 1 {
                    let qm = win(&qmk) & lm;
                    // A field is valid iff it has no quote, or '"' exactly at its first and last
                    // byte and length >= 2. Line-wide part: every quote sits on a field's first
                    // byte (after a comma or at 0) or last byte (before a comma or the end).
                    let starts = (cm << 1) | 1;
                    let lasts = (cm | (1u128 << e)) >> 1;
                    // Per field, with masks only: bit j + 1 of dl marks delimiter j (bit 0 is a
                    // virtual delimiter at -1, then the commas, then the line end). Compacted over
                    // the delimiters (pext), bit k of qa = "field k starts with a quote" and bit
                    // k + 1 of qb = "field k ends with a quote"; they must agree for every field.
                    // A field that is a lone '"' has delimiters right before and after it.
                    let dl = ((cm | (1u128 << e)) << 1) | 1;
                    let qa = pext128(qm & dl, dl);
                    let qb = pext128((qm << 2) & dl, dl);
                    let lone = (qm << 1) & (dl << 1) & (dl >> 1);
                    let ok = ((qm & !(starts | lasts)) == 0) & (((qa ^ (qb >> 1)) & ((1u64 << N) - 1)) == 0) & (lone == 0);
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
                        let qs = ((qa >> k) & 1) as usize;
                        let a = s + qs;
                        let b = (fe - qs).max(a);
                        out[k] = unsafe { std::slice::from_raw_parts(base.add(p + a), b - a) };
                        s = fe + 1;
                    }
                    p += e + 1;
                    break 'row ok;
                }
                p += e + 1;
                break 'row false;
            }
        }
        let e = match memchr::memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => len,
        };
        let g = split(&data[p..e], &mut out);
        p = e + 1;
        g
        };
        if good {
            f(&out);
        }
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
        ((days_fast(y, mo, dd) * 24 + h) * 60 + mi) * 60_000 + se * 1000
    } else {
        parse_ts(s).unwrap_or(i64::MAX / 4)
    }
}

// ------------------------------------------------------------------ timestamps
#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some((a as i64) * 10 + b as i64)
    } else {
        None
    }
}

/// days_from_civil(y, m, 1) for y in [MY0, MY0 + MYN), m in 1..=12: no divisions on the hot path.
const MY0: i64 = 1900;
const MYN: usize = 400;
static MONTH0: [i64; MYN * 12] = {
    let mut t = [0i64; MYN * 12];
    let mut i = 0;
    while i < MYN * 12 {
        t[i] = days_from_civil(MY0 + (i / 12) as i64, (i % 12) as i64 + 1, 1);
        i += 1;
    }
    t
};

/// Days in month for y in [MY0, MY0 + MYN), m in 1..=12 (index (y - MY0) * 12 + m - 1).
static DIM0: [u8; MYN * 12] = {
    let mut t = [0u8; MYN * 12];
    let mut i = 0;
    while i < MYN * 12 {
        let y = MY0 + (i / 12) as i64;
        let m = (i % 12) as i64 + 1;
        t[i] = (days_from_civil(if m == 12 { y + 1 } else { y }, if m == 12 { 1 } else { m + 1 }, 1) - days_from_civil(y, m, 1)) as u8;
        i += 1;
    }
    t
};

/// F1/F2 digit fields from the 8-byte words at offsets 0 ("YYYY-MM-"), 8 ("DDTHH:MM") and
/// 11 ("HH:MM:SS"), SWAR: separators masked off, '0' subtracted only at digit bytes (no
/// borrows when they are digits), then byte i becomes 10 * d[i] + d[i + 1].
/// Caller must have checked the digit bytes (`f1_digits_ok`).
#[inline(always)]
fn f1_digits(a: u64, b: u64, c: u64) -> (i64, i64, i64, i64, i64, i64) {
    const Z: u64 = 0x3030_3030_3030_3030;
    const MA: u64 = 0x00FF_FF00_FFFF_FFFF;
    const MB: u64 = 0xFFFF_00FF_FF00_FFFF;
    let pr = |x: u64, m: u64| {
        let v = (x & m).wrapping_sub(Z & m);
        v.wrapping_mul(10).wrapping_add(v >> 8)
    };
    let ta = pr(a, MA);
    let tb = pr(b, MB);
    let tc = pr(c, MB);
    let by = |t: u64, i: u32| ((t >> (8 * i)) & 0xFF) as i64;
    (by(ta, 0) * 100 + by(ta, 2), by(ta, 5), by(tb, 0), by(tc, 0), by(tc, 3), by(tc, 6))
}

/// All F1/F2 digit positions of the three words are ASCII digits.
#[inline(always)]
fn f1_digits_ok(a: u64, b: u64, c: u64) -> bool {
    const Z: u64 = 0x3030_3030_3030_3030;
    const MA: u64 = 0x00FF_FF00_FFFF_FFFF;
    const MB: u64 = 0xFFFF_00FF_FF00_FFFF;
    dig8((a & MA) | (Z & !MA)) & dig8((b & MB) | (Z & !MB)) & dig8((c & MB) | (Z & !MB))
}

/// days_from_civil(y, m, d), via the month table when y and m are in its range.
#[inline(always)]
fn days_fast(y: i64, m: i64, d: i64) -> i64 {
    let yi = y - MY0;
    if (0..MYN as i64).contains(&yi) && (1..=12).contains(&m) {
        MONTH0[yi as usize * 12 + (m - 1) as usize] + d - 1
    } else {
        days_from_civil(y, m, d)
    }
}

const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
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
            let (y, mo, dd, h, mi, se) = f1_digits(a, b, c);
            if !(1..=12).contains(&mo) {
                return None;
            }
            let yi = y - MY0;
            let (dim, d0) = if (0..MYN as i64).contains(&yi) {
                let i = yi as usize * 12 + (mo - 1) as usize;
                (DIM0[i] as i64, MONTH0[i])
            } else {
                let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
                let dim = match mo {
                    2 => 28 + leap as i64,
                    4 | 6 | 9 | 11 => 30,
                    _ => 31,
                };
                (dim, days_from_civil(y, mo, 1))
            };
            if (dd < 1) | (dd > dim) | (h > 23) | (mi > 59) | (se > 59) {
                return None;
            }
            let base = (((d0 + dd - 1) * 24 + h) * 60 + mi) * 60_000 + se * 1000;
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

/// Calendar months that can hold an in-window timestamp: every month touched by the days
/// from (first window day - 1) to (last window day + 1), so an F2 offset (< 24 h) cannot move
/// a timestamp of any other month into the window. Each month: its "YYYY-MM-" bytes, the
/// epoch ms of its day 1, and its length in days.
struct Win {
    pre: [u64; 8],
    base: [i64; 8],
    bsec: [i64; 8],
    dim: [i64; 8],
    n: usize,
}

impl Win {
    /// None when the span needs more than 8 months (the caller keeps the general parser).
    fn new(lo_ms: i64, hi_ms: i64) -> Option<Win> {
        let mut w = Win { pre: [u64::MAX; 8], base: [0; 8], bsec: [0; 8], dim: [0; 8], n: 0 };
        let d0 = lo_ms.div_euclid(DAY) - 1;
        let d1 = (hi_ms - 1).div_euclid(DAY) + 1;
        let mut d = d0;
        while d <= d1 {
            let (y, m, _) = civil_from_days(d);
            if !(0..=9999).contains(&y) {
                return None;
            }
            let txt = format!("{:04}-{:02}-", y, m);
            let p = u64::from_le_bytes(txt.as_bytes().try_into().unwrap());
            if w.n == 0 || w.pre[w.n - 1] != p {
                if w.n == 8 {
                    return None;
                }
                let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
                w.pre[w.n] = p;
                w.base[w.n] = days_from_civil(y, m, 1) * DAY;
                w.bsec[w.n] = days_from_civil(y, m, 1) * 86_400;
                w.dim[w.n] = match m {
                    2 => 28 + leap as i64,
                    4 | 6 | 9 | 11 => 30,
                    _ => 31,
                };
                w.n += 1;
            }
            d += 1;
        }
        Some(w)
    }
    #[inline(always)]
    fn month(&self, a: u64) -> Option<usize> {
        (0..self.n).find(|&i| self.pre[i] == a)
    }
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// F1 tail "-MM-DDTHH:MM:SS" + 'Z' (bytes 4..20 of a 20-byte F1 value), SSSE3: every digit byte
/// a digit and every separator exact, then (dd, minutes of the day, ss, hh/mi/ss in range).
/// None when a byte does not match. `rng` is false when hh > 23, mi > 59 or ss > 59.
#[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
#[inline(always)]
fn f1_tail(s: &[u8]) -> Option<(i64, i64, i64, bool)> {
    debug_assert!(s.len() == 20);
    unsafe {
        use std::arch::x86_64::*;
        let c = _mm_loadu_si128(s.as_ptr().add(4) as *const __m128i);
        // byte ^ template: digit positions become their value (0..9 iff a digit), separators 0
        let t = _mm_setr_epi8(b'-' as i8, 48, 48, b'-' as i8, 48, 48, b'T' as i8, 48, 48, b':' as i8, 48, 48, b':' as i8, 48, 48, b'Z' as i8);
        let l = _mm_setr_epi8(0, 9, 9, 0, 9, 9, 0, 9, 9, 0, 9, 9, 0, 9, 9, 0);
        let x = _mm_xor_si128(c, t);
        if _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(x, l), x)) != 0xFFFF {
            return None;
        }
        // u16 lanes [hh, mi, ss, 0, dd, 0, 0, 0]
        let g = _mm_shuffle_epi8(x, _mm_setr_epi8(7, 8, 10, 11, 13, 14, -128, -128, 4, 5, -128, -128, -128, -128, -128, -128));
        let v = _mm_maddubs_epi16(g, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 0, 0, 10, 1, 0, 0, 0, 0, 0, 0));
        let rng = _mm_movemask_epi8(_mm_cmpgt_epi16(v, _mm_setr_epi16(23, 59, 59, 0, 99, 0, 0, 0))) == 0;
        // i32 lanes [hh * 60 + mi, ss, dd, 0]
        let w = _mm_madd_epi16(v, _mm_setr_epi16(60, 1, 1, 0, 1, 0, 0, 0));
        let lo = _mm_cvtsi128_si64(w) as u64;
        let dd = _mm_extract_epi32::<2>(w) as i64;
        Some((dd, (lo & 0xFFFF_FFFF) as i64, (lo >> 32) as i64, rng))
    }
}

/// `parse_ts_fast` restricted to a window: equal to it on every timestamp whose value lies
/// in [lo, hi) of the `Win` it was built for; may return None (instead of a value) for an
/// F1/F2 timestamp outside that span, which the caller's window check drops anyway.
#[inline(always)]
fn parse_ts_win(s: &[u8], w: &Win) -> Option<i64> {
    let n = s.len();
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
    if n == 20 && s[19] == b'Z' {
        let mi_ = w.month(ld8(s, 0))?; // "YYYY-MM-" of a candidate month (so valid digits)
        let (dd, mins, se, rng) = f1_tail(s)?;
        if !rng | (dd < 1) | (dd > w.dim[mi_]) {
            return None;
        }
        return Some(w.base[mi_] + (dd - 1) * DAY + (mins * 60 + se) * 1000);
    }
    if n == 20 || n == 25 {
        let is_f1 = n == 20 && s[19] == b'Z';
        let is_f2 = n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':';
        if !(is_f1 || is_f2) {
            return parse_ts(s);
        }
        let a = ld8(s, 0);
        let mi_ = w.month(a)?; // "YYYY-MM-" of a candidate month (so valid digits)
        // bytes 8..16 "DDTHH:MM", 11..19 "HH:MM:SS"
        let b = ld8(s, 8);
        let c = ld8(s, 11);
        const SB: u64 = 0xFF << 16;
        const PB: u64 = (b'T' as u64) << 16;
        const SC: u64 = 0xFF << 16 | 0xFF << 40;
        const PC: u64 = (b':' as u64) << 16 | (b':' as u64) << 40;
        const Z: u64 = 0x3030_3030_3030_3030;
        if !((b & SB) == PB && (c & SC) == PC && dig8((b & !SB & 0xFFFF_FFFF) | (Z & SB) | (Z & !0xFFFF_FFFF)) && dig8((c & !SC) | (Z & SC))) {
            return None;
        }
        let (_, _, dd, h, mi, se) = f1_digits(a, b, c);
        if (dd < 1) | (dd > w.dim[mi_]) | (h > 23) | (mi > 59) | (se > 59) {
            return None;
        }
        let base = w.base[mi_] + (dd - 1) * DAY + ((h * 60 + mi) * 60 + se) * 1000;
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
    parse_ts_fast(s)
}

/// `ingest_ms` with the month looked up in a `Win` (same value; other months keep `ingest_ms`).
#[inline(always)]
fn ingest_ms_win(s: &[u8], w: &Win) -> i64 {
    ingest_win(s, w).0
}

/// (`ingest_ms`, whether it is a whole second, its value in seconds (floor)). F1 values in a
/// `Win` month with all-digit fields go through SWAR (f1_digits) and need no division.
#[inline(always)]
fn ingest_win(s: &[u8], w: &Win) -> (i64, bool, i64) {
    if s.len() == 20 && s[19] == b'Z' {
        if let Some(m) = w.month(ld8(s, 0)) {
            #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
            if let Some((dd, mins, se, _)) = f1_tail(s) {
                let sec = w.bsec[m] + (dd - 1) * 86_400 + mins * 60 + se;
                return (sec * 1000, true, sec);
            }
            #[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1")))]
            {
                let (a, b, c) = (ld8(s, 0), ld8(s, 8), ld8(s, 11));
                if f1_digits_ok(a, b, c) {
                    let (_, _, dd, h, mi, se) = f1_digits(a, b, c);
                    let sec = w.bsec[m] + (dd - 1) * 86_400 + (h * 60 + mi) * 60 + se;
                    return (sec * 1000, true, sec);
                }
            }
        }
    }
    let ms = ingest_ms(s);
    (ms, ms.rem_euclid(1000) == 0, ms.div_euclid(1000))
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

/// An input file read with pread (fast path): no file mapping, so no page-table entries to
/// populate while parsing or to zap at exit.
struct InFile {
    f: std::fs::File,
    len: usize,
}

static FADV: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn open_inputs(paths: &[PathBuf]) -> Vec<InFile> {
    paths
        .iter()
        .filter_map(|p| {
            let f = std::fs::File::open(p).ok()?;
            let len = f.metadata().ok()?.len() as usize;
            #[cfg(target_os = "linux")]
            if FADV.load(Ordering::Relaxed) {
                // start async readahead of the whole file (cold page cache)
                use std::os::unix::io::AsRawFd;
                unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, len as i64, libc::POSIX_FADV_WILLNEED) };
            }
            (len > 0).then_some(InFile { f, len })
        })
        .collect()
}

/// Asynchronous readahead of [s, e) into the page cache (POSIX_FADV_WILLNEED; a hint).
fn readahead(f: &InFile, s: usize, e: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        use std::os::unix::io::AsRawFd;
        libc::posix_fadvise(f.f.as_raw_fd(), s as i64, (e - s) as i64, libc::POSIX_FADV_WILLNEED);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (f, s, e);
}

fn read_exact_at(f: &std::fs::File, buf: &mut [u8], off: usize) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        f.read_exact_at(buf, off as u64).expect("pread");
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            let n = f.seek_read(&mut buf[done..], (off + done) as u64).expect("pread");
            assert!(n > 0, "short read");
            done += n;
        }
    }
}

/// A worker's reusable read buffer (leaked; grown only for lines longer than the slack).
static FIRST_READ: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
struct RdBuf {
    p: *mut u8,
    cap: usize,
}
unsafe impl Send for RdBuf {}
const RD_PAD: usize = 256; // readable bytes past the data (SIMD loads, pack16's 16-byte load)

impl RdBuf {
    fn new(cap: usize) -> RdBuf {
        let v: &'static mut [u8] = Box::leak(vec![0u8; cap].into_boxed_slice());
        RdBuf { p: v.as_mut_ptr(), cap }
    }
    /// Read the bytes for the lines that start in [s, e) of `f`, plus the byte before s (the
    /// line-start rule of `for_rows`). Returns (data, start, end, lim) for `for_rows` and pack16.
    /// The data is valid until the next read into this buffer.
    fn piece(&mut self, f: &InFile, s: usize, e: usize) -> (S, usize, usize, usize) {
        let rs = s.saturating_sub(1);
        let mut want = (e - rs + 4096).min(f.len - rs);
        loop {
            if want + RD_PAD > self.cap {
                *self = RdBuf::new((want + RD_PAD) * 2);
            }
            let b = unsafe { std::slice::from_raw_parts_mut(self.p, want) };
            if TL.load(Ordering::Relaxed) && !FIRST_READ.swap(true, Ordering::Relaxed) {
                tlog("first pread");
            }
            read_exact_at(&f.f, b, rs);
            // every line starting before e ends at the first '\n' at or after e - 1
            if rs + want == f.len || memchr::memchr(b'\n', &b[e - 1 - rs..]).is_some() {
                break;
            }
            want = (want * 2).min(f.len - rs);
        }
        let d: S = unsafe { std::slice::from_raw_parts(self.p, want) };
        (d, s - rs, e - rs, self.p as usize + self.cap)
    }
}

fn chunks_in(files: &[InFile], ch: usize) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    for (i, f) in files.iter().enumerate() {
        let mut s = 0;
        while s < f.len {
            let e = (s + ch).min(f.len);
            out.push((i, s, e));
            s = e;
        }
    }
    out
}

/// `idk_long` for a transient slice: the interned copy is owned (leaked) instead of borrowed.
#[inline(never)]
fn idk_long_own(s: &[u8]) -> K {
    let sh = (hs(s) >> 58) as usize;
    let mut g = LONG[sh].lock().unwrap();
    let l = g.get_or_insert_with(|| LongIds { map: Default::default(), list: Vec::new() });
    let i = match l.map.get(s) {
        Some(&i) => i,
        None => {
            let o: S = Box::leak(s.to_vec().into_boxed_slice());
            let i = l.list.len() as u32;
            l.list.push(o);
            l.map.insert(o, i);
            i
        }
    };
    ((((i as u64) << 6) | sh as u64) << 4) | 0xF
}

/// `idkf` for pread buffers (long IDs are copied when first interned).
#[inline(always)]
fn idkf_own(s: S, lim: usize) -> K {
    match kfast(s, lim) {
        Some(k) => k,
        None if s.is_empty() => 0,
        None => idk_long_own(s),
    }
}

/// Map the chunk's page-cache pages in one call (MADV_POPULATE_READ, Linux >= 5.14) instead of
/// one fault per fault-around window. Errors (older kernels) are ignored: it is only a hint.
#[inline]
fn populate(d: S, s: usize, e: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        if POPULATE.load(Ordering::Relaxed) {
            let a = (d.as_ptr() as usize + s) & !4095;
            let b = d.as_ptr() as usize + e;
            libc::madvise(a as *mut libc::c_void, b - a, 22 /* MADV_POPULATE_READ */);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (d, s, e);
}
static POPULATE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

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
/// Interner for IDs that do not pack: hash-sharded (shard = top bits of a full-bytes hash),
/// one mutex per shard, so parallel parsers rarely contend. Keys carry (index, shard); their
/// numeric value is never used for ordering (`id_cmp` compares bytes), so outputs do not
/// depend on insertion order.
const NSHARD: usize = 64;
static LONG: [std::sync::Mutex<Option<LongIds>>; NSHARD] = [const { std::sync::Mutex::new(None) }; NSHARD];
static LONG_LIST: std::sync::OnceLock<Vec<Vec<S>>> = std::sync::OnceLock::new();

#[inline]
fn idk(s: S) -> K {
    if s.is_empty() {
        return 0;
    }
    match kfast_s(s) {
        Some(k) => k,
        None => idk_long(s),
    }
}

/// Fast-shape key (z23): an ID of 2..=9 bytes, all ASCII (1..=0x7F, no NUL) becomes
/// bit 63 | first byte << 56 | the other (up to 8) bytes as 7-bit fields, first most
/// significant, zero padded. Integer order of two fast keys == byte-wise order of the IDs
/// (no NUL, so the zero padding sorts a prefix first). `x` = the 8 bytes after the first,
/// big-endian (bytes past the ID are masked off here).
#[inline(always)]
fn kfast_x(c0: u64, x: u64, n: usize) -> Option<K> {
    const L: u64 = 0x0101_0101_0101_0101;
    const H: u64 = 0x8080_8080_8080_8080;
    let m = u64::MAX << (8 * (9 - n)); // the n - 1 ID bytes (n in 2..=9)
    let y = x & m;
    let t = y | (!m & L); // padding bytes -> 0x01: only ID bytes can be 0
    let bad = ((t.wrapping_sub(L) & !t) | t) & H; // a zero byte or a byte >= 0x80
    if (bad == 0) & (c0.wrapping_sub(1) < 0x7F) {
        #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
        let v = unsafe { core::arch::x86_64::_pext_u64(y, 0x7F7F_7F7F_7F7F_7F7F) };
        #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
        let v = {
            let mut v = 0u64;
            for j in 0..8 {
                v = (v << 7) | ((y >> (56 - 8 * j)) & 0x7F);
            }
            v
        };
        return Some((1u64 << 63) | (c0 << 56) | v);
    }
    None
}

/// `kfast_x` from the slice alone (no read past it).
#[inline(always)]
fn kfast_s(s: S) -> Option<K> {
    let n = s.len();
    if n.wrapping_sub(2) < 8 {
        let mut b = [0u8; 8];
        b[..n - 1].copy_from_slice(&s[1..]);
        return kfast_x(s[0] as u64, u64::from_be_bytes(b), n);
    }
    None
}

/// `kfast_s` with one unaligned 8-byte load when the 9 bytes from s are readable (s + 9 <= lim).
#[inline(always)]
fn kfast(s: S, lim: usize) -> Option<K> {
    let n = s.len();
    let p = s.as_ptr();
    if (n.wrapping_sub(2) < 8) & (p as usize + 9 <= lim) {
        let x = u64::from_be(unsafe { std::ptr::read_unaligned(p.add(1) as *const u64) });
        return kfast_x(unsafe { *p } as u64, x, n);
    }
    kfast_s(s)
}

/// The packed key of an ID, or None when it does not pack (see `K`).
#[inline(always)]
fn pack(s: S) -> Option<K> {
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
            return Some(((code << (6 * (10 - n))) << 4) | n as u64);
        }
    }
    None
}

/// LENMASK16[16 - n..32 - n]: n bytes 0xFF then zeros (n <= 16).
static LENMASK16: [u8; 32] = {
    let mut t = [0u8; 32];
    let mut i = 0;
    while i < 16 {
        t[i] = 0xFF;
        i += 1;
    }
    t
};

/// `pack` with SIMD rank mapping + pext (same result). `lim` is the end of the mapping that
/// holds `s`: the 16-byte load needs s.as_ptr() + 16 <= lim, else the scalar `pack` runs.
#[inline(always)]
fn pack16(s: S, lim: usize) -> Option<K> {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2", target_feature = "sse4.1"))]
    {
        let n = s.len();
        let p = s.as_ptr() as usize;
        if n <= 10 && p + 16 <= lim {
            unsafe {
                use std::arch::x86_64::*;
                let c = _mm_loadu_si128(p as *const __m128i);
                // nibble classification (simdjson style): a byte packs iff the class bits of its
                // high nibble and of its low nibble intersect. Classes: 1 '-' (2D), 2 digits
                // (30-39), 4 41-4F|61-6F, 8 50-5A|70-7A, 16 '_' (5F). Bytes >= 0x80 have a
                // high nibble >= 8 (class 0): they never pack.
                let ht = _mm_setr_epi8(0, 0, 1, 2, 4, 8 | 16, 4, 8, 0, 0, 0, 0, 0, 0, 0, 0);
                let lt = _mm_setr_epi8(2 | 8, 14, 14, 14, 14, 14, 14, 14, 14, 14, 4 | 8, 4, 4, 1 | 4, 4, 4 | 16);
                // byte - rank by high nibble ('-' 45, digits 47, A-Z 54, a-z 59); '_' is 58 = 54 + 4
                let ot = _mm_setr_epi8(0, 0, 45, 47, 54, 54, 59, 59, 0, 0, 0, 0, 0, 0, 0, 0);
                let nib = _mm_set1_epi8(0x0F);
                let hi = _mm_and_si128(_mm_srli_epi16::<4>(c), nib);
                let lo = _mm_and_si128(c, nib);
                let cls = _mm_and_si128(_mm_shuffle_epi8(ht, hi), _mm_shuffle_epi8(lt, lo));
                let inlen = _mm_loadu_si128(LENMASK16.as_ptr().add(16 - n) as *const __m128i);
                if _mm_movemask_epi8(_mm_and_si128(_mm_cmpeq_epi8(cls, _mm_setzero_si128()), inlen)) != 0 {
                    return None; // a byte outside [-0-9A-Z_a-z]: does not pack (as `pack`)
                }
                let us = _mm_and_si128(_mm_cmpeq_epi8(c, _mm_set1_epi8(b'_' as i8)), _mm_set1_epi8(4));
                let r = _mm_and_si128(_mm_sub_epi8(_mm_sub_epi8(c, _mm_shuffle_epi8(ot, hi)), us), inlen);
                let lo64 = _mm_cvtsi128_si64(r) as u64;
                let hi64 = _mm_extract_epi64::<1>(r) as u64;
                let vlo = _pext_u64(lo64.swap_bytes(), 0x3F3F_3F3F_3F3F_3F3F); // ranks 0..7, first most significant
                let vhi = _pext_u64((hi64 as u16).swap_bytes() as u64, 0x3F3F); // ranks 8, 9
                return Some((((vlo << 12) | vhi) << 4) | n as u64);
            }
        }
    }
    let _ = lim;
    pack(s)
}

#[inline(never)]
fn idk_long(s: S) -> K {
    let sh = (hs(s) >> 58) as usize;
    let mut g = LONG[sh].lock().unwrap();
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
    ((((i as u64) << 6) | sh as u64) << 4) | 0xF
}

/// Freeze the long-ID list (call once all parsing is done).
fn freeze_long() {
    let v: Vec<Vec<S>> = LONG.iter().map(|m| m.lock().unwrap().as_ref().map(|l| l.list.clone()).unwrap_or_default()).collect();
    let _ = LONG_LIST.set(v);
}

/// Interned key (not fast-shape, not the empty ID 0).
#[inline]
fn is_long(k: K) -> bool {
    (k >> 63 == 0) & (k != 0)
}

/// Bytes of an ID key. Long keys need `freeze_long` (or the interner) to have run.
fn long_bytes(k: K) -> S {
    let i = (k >> 4) as usize;
    let (sh, idx) = (i & (NSHARD - 1), i >> 6);
    match LONG_LIST.get() {
        Some(v) if idx < v[sh].len() => v[sh][idx],
        _ => LONG[sh].lock().unwrap().as_ref().unwrap().list[idx],
    }
}

fn id_bytes(k: K, out: &mut Vec<u8>) {
    if is_long(k) {
        out.extend_from_slice(long_bytes(k));
    } else if k != 0 {
        let mut b = [0u8; 9];
        let n = fast_bytes(k, &mut b);
        out.extend_from_slice(&b[..n]);
    }
}

/// Bytes of a fast key (k >> 63 == 1) into b; returns the length.
#[inline]
fn fast_bytes(k: K, b: &mut [u8; 9]) -> usize {
    b[0] = ((k >> 56) & 0x7F) as u8;
    let mut n = 1;
    while n < 9 {
        let c = ((k >> (7 * (8 - n))) & 0x7F) as u8;
        if c == 0 {
            break;
        }
        b[n] = c;
        n += 1;
    }
    n
}

#[inline]
fn id_cmp(a: K, b: K) -> std::cmp::Ordering {
    if (a & b) >> 63 == 1 {
        return a.cmp(&b); // two fast keys: integer order is byte order
    }
    // no allocation: long IDs are interned slices, fast ones decode into a stack buffer
    fn bytes(k: K, buf: &mut [u8; 9]) -> &[u8] {
        if is_long(k) {
            return long_bytes(k);
        }
        if k == 0 {
            return &buf[..0];
        }
        let n = fast_bytes(k, buf);
        &buf[..n]
    }
    let (mut x, mut y) = ([0u8; 9], [0u8; 9]);
    bytes(a, &mut x).cmp(bytes(b, &mut y))
}

/// Table-slot hash for the per-partition tables (dedup, 2b link table): one multiply, slot
/// bits taken from 24.. (independent of `ph`'s partition bits: other constant, other fold).
#[inline(always)]
fn ht(k: K) -> u64 {
    (k ^ (k >> 32)).wrapping_mul(0xd6e8_feb8_6659_fd93) >> 24
}

/// Partition hash: bits 40.. pick a partition (one multiply; the tables inside a partition
/// probe with the independent `h128`).
#[inline(always)]
fn ph(k: K) -> u64 {
    (k ^ (k >> 31)).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

/// One cache line of 4 (id key, person key) pairs; key 0 marks an empty pair.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct KB8([K; 8]);

/// (lanes whose key equals k, lanes whose key is empty), as bits 0/2/4/6 of the 8 lanes
#[inline(always)]
fn kb8_pairs(b: &KB8, k: K) -> (u32, u32) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let p = b.0.as_ptr() as *const __m256i;
        let (x0, x1) = (_mm256_load_si256(p), _mm256_load_si256(p.add(1)));
        let kk = _mm256_set1_epi64x(k as i64);
        let z = _mm256_setzero_si256();
        let mm = |a: __m256i, c: __m256i| _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(a, c))) as u32;
        (((mm(x0, kk) | (mm(x1, kk) << 4)) & 0x55), ((mm(x0, z) | (mm(x1, z) << 4)) & 0x55))
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut h, mut e) = (0u32, 0u32);
        for j in 0..4 {
            h |= ((b.0[2 * j] == k) as u32) << (2 * j);
            e |= ((b.0[2 * j] == 0) as u32) << (2 * j);
        }
        (h, e)
    }
}

/// `if c != 0 { a } else { b }` as a cmov: LLVM turns a plain select (or the mask form) into a
/// branch here, and on a data-random condition that branch mispredicts about half the time.
#[inline(always)]
fn sel_nz(c: u32, a: u64, b: u64) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        let mut r = b;
        unsafe { std::arch::asm!("test {c:e}, {c:e}", "cmovnz {r}, {a}", c = in(reg) c, a = in(reg) a, r = inout(reg) r, options(pure, nomem, nostack)) };
        r
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        if c != 0 { a } else { b }
    }
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

/// Dedup stream: one per event copy, bucketed by hash(event_id). `loc` addresses the copy's
/// record in its worker's valid-copy stream (worker << 40 | index), or NOLOC when the copy
/// itself failed validation (it can still win, which drops the event).
#[derive(Clone, Copy)]
struct DRec {
    key: K,
    ing: i64,
    loc: u64,
}
const NOLOC: u64 = u64::MAX;

/// Dedup record of an event_id that does not pack. event_ids only need equality (they are
/// never output or ordered), so instead of interning them they carry a hash plus their bytes
/// (a slice of the input mapping), and dedup confirms every hash match byte for byte.
#[derive(Clone, Copy)]
struct DRecL {
    h: u64, // never 0 (0 marks an empty table slot)
    ing: i64,
    loc: u64,
    ptr: usize,
    len: usize,
}

/// Surviving event, bucketed by hash(person). Also the valid-copy stream record (pk = user key).
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

/// Compact records of the fast path (main_fast). Times are u32 ms offsets from
/// cw0 = window_start - 7 d (static precondition: window_end - cw0 < 2^32 ms); pc = price << 8 | category
/// rank for purchases and 0 for view/cart (price >= 1, so a purchase never has pc 0; static
/// precondition: <= 256 categories).
#[derive(Clone, Copy)]
struct DRec16 {
    key: K,
    ing: u32, // ingest seconds - ibase (copies that do not fit go to the wide stream)
    loc: u32, // NOLOC32 or (w << (qb + ib)) | (q << ib) | idx
}
const NOLOC32: u32 = u32::MAX;

/// A valid event copy without its purchase (12 bytes): pk, ts offset. Purchases (pc != 0)
/// go to a side stream of (index in the partition, pc) in index order.
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct PRec12 {
    pk: K,
    ts: u32,
}

#[derive(Clone, Copy)]
struct PRec16 {
    pk: K,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy)]
struct CRec24 {
    pk: K, // raw user key before the person map
    cid: K,
    ts: u32,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Ck16 {
    cid: K,
    ts: u32,
    camp: u32,
}

/// Linked IDs -> person key: open addressing (linear probing) over (id key, person key).
struct Links {
    tab: &'static [(K, K)], // key 0 (the empty string) marks an empty slot; empty IDs are never linked
    mask: usize,
    n: usize,
    // the same (id, person) entries split by hash(id) partition (bits 40.. & pmask), so a
    // phase-2b task joins one user partition against a cache-resident table
    parts: Vec<Vec<(K, K)>>,
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

fn build_links(lk_files: &[S], np: usize) -> Links {
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
    while sz < pairs.len() * 2 {
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
    let c2 = tcpu();
    let n = ids.len();
    let mut best: Vec<u32> = (0..n as u32).collect(); // per root: index of the byte-wise smallest id
    for i in 0..n as u32 {
        let r = find(&mut parent, i) as usize;
        if id_cmp(ids[i as usize], ids[best[r] as usize]).is_lt() {
            best[r] = i;
        }
    }
    let c3 = tcpu();
    // rewrite values in place: id index -> person name (sequential sweep over the table)
    for e in t.iter_mut() {
        if e.0 != 0 {
            let r = find(&mut parent, e.1 as u32);
            e.1 = ids[best[r as usize] as usize];
        }
    }
    log(&format!("links built: {} pairs, cpu parse {:.3} s, union-find + table {:.3} s (intern+union {:.3}, best {:.3}, rewrite {:.3})", pairs.len(), c1 - c0, tcpu() - c1, c2 - c1, c3 - c2, tcpu() - c3));
    // exact-capacity partitions (count, then fill): no growth copies
    let mut cnt = vec![0usize; np];
    for &e in t.iter() {
        if e.0 != 0 {
            cnt[((ph(e.0) >> 40) as usize) & (np - 1)] += 1;
        }
    }
    let mut parts: Vec<Vec<(K, K)>> = cnt.iter().map(|&c| Vec::with_capacity(c)).collect();
    for &e in t.iter() {
        if e.0 != 0 {
            parts[((ph(e.0) >> 40) as usize) & (np - 1)].push(e);
        }
    }
    Links { tab: t, mask, n, parts }
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

// ------------------------------------------------------------------ phase-3 sessions
/// Time buckets of width GAP: events in one bucket are at most GAP - 1 ms apart, so they always share
/// a session; buckets that are not adjacent are more than GAP apart, so they never do. A person's
/// sessions therefore follow from per-bucket (min, max, count) in bucket order, with no sort.
/// Event ts are u32 offsets, so NB buckets cover every value.
const NB: usize = ((1u64 << 32) / GAP as u64 + 1) as usize;
const NBW: usize = NB.div_ceil(64);

/// Per-worker bucket state. Between calls every bucket is empty (min u32::MAX, max 0, count 0, bit 0).
struct Bk {
    bmin: Vec<u32>,
    bmax: Vec<u32>,
    bcnt: Vec<u32>,
    bits: Vec<u64>,
}

impl Bk {
    fn new() -> Self {
        Bk { bmin: vec![u32::MAX; NB], bmax: vec![0; NB], bcnt: vec![0; NB], bits: vec![0; NBW] }
    }
}

/// One person's sessions from events (ts << 32 | pc, any order): `ses` gets (start, end, events) in
/// time order, `pur` the purchase events (pc != 0) sorted ascending.
fn sessions_bucket(e: &[u64], bk: &mut Bk, ses: &mut Vec<(u32, u32, u32)>, pur: &mut Vec<u64>) {
    ses.clear();
    pur.clear();
    let (mut lo, mut hi) = (NBW, 0usize);
    let (bmin, bmax, bcnt, bits) = (&mut bk.bmin[..NB], &mut bk.bmax[..NB], &mut bk.bcnt[..NB], &mut bk.bits[..NBW]);
    for &x in e {
        let ts = (x >> 32) as u32;
        // ts < 2^32, so b < NB and w < NBW
        let b = (ts / GAP as u32) as usize;
        let w = b >> 6;
        unsafe {
            *bits.get_unchecked_mut(w) |= 1u64 << (b & 63);
            let m = bmin.get_unchecked_mut(b);
            *m = (*m).min(ts);
            let m = bmax.get_unchecked_mut(b);
            *m = (*m).max(ts);
            *bcnt.get_unchecked_mut(b) += 1;
        }
        lo = lo.min(w);
        hi = hi.max(w);
        if x as u32 != 0 {
            pur.push(x);
        }
    }
    let gap = GAP as u32;
    let mut run_max = 0u32;
    for w in lo..=hi {
        let mut bw = bits[w];
        bits[w] = 0;
        while bw != 0 {
            let b = w * 64 + bw.trailing_zeros() as usize;
            bw &= bw - 1;
            let (mn, mx, c) = (bmin[b], bmax[b], bcnt[b]);
            (bmin[b], bmax[b], bcnt[b]) = (u32::MAX, 0, 0);
            // buckets ascend, so mn > run_max of every earlier bucket
            if ses.is_empty() || mn - run_max > gap {
                ses.push((mn, mx, c));
            } else {
                let l = ses.len() - 1;
                ses[l].1 = mx;
                ses[l].2 += c;
            }
            run_max = mx;
        }
    }
    if pur.len() > 1 {
        pur.sort_unstable();
    }
}

/// Phase-3 person interning: open addressing (linear probing) with the key inline, so a hit is one
/// probe of one 16 B slot. pid u32::MAX marks an empty slot. Grows at load 1/2.
struct Itab {
    t: Vec<(K, u32)>,
    mask: usize,
}

impl Itab {
    fn new() -> Self {
        Itab { t: Vec::new(), mask: 0 }
    }
    fn reset(&mut self, n: usize) {
        let sz = (n * 2).next_power_of_two().max(64);
        self.t.clear();
        self.t.resize(sz, (0, u32::MAX));
        self.mask = sz - 1;
    }
    /// pid of k, inserting it as `names.len()` when absent
    #[inline]
    fn intern(&mut self, k: K, names: &mut Vec<K>) -> u32 {
        let mut i = h128(k) as usize & self.mask;
        loop {
            // i <= mask < t.len()
            let e = unsafe { self.t.get_unchecked_mut(i) };
            if e.1 == u32::MAX {
                let pid = names.len() as u32;
                *e = (k, pid);
                names.push(k);
                if names.len() * 2 > self.mask + 1 {
                    self.grow(names);
                }
                return pid;
            }
            if e.0 == k {
                return e.1;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn find(&self, k: K) -> u32 {
        let mut i = h128(k) as usize & self.mask;
        loop {
            let e = unsafe { self.t.get_unchecked(i) };
            if e.1 == u32::MAX || e.0 == k {
                return e.1;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self, names: &[K]) {
        self.reset(names.len() * 2);
        for (pid, &k) in names.iter().enumerate() {
            let mut i = h128(k) as usize & self.mask;
            while self.t[i].1 != u32::MAX {
                i = (i + 1) & self.mask;
            }
            self.t[i] = (k, pid as u32);
        }
    }
}

/// Reference path (sort + gap sweep), same contract as `sessions_bucket`; P3SORT=1 uses it.
fn sessions_sort(e: &mut [u64], ses: &mut Vec<(u32, u32, u32)>, pur: &mut Vec<u64>) {
    ses.clear();
    pur.clear();
    e.sort_unstable();
    let gap = GAP as u32;
    let mut i = 0;
    while i < e.len() {
        let start = (e[i] >> 32) as u32;
        let mut prev = start;
        let mut j = i;
        // equal timestamps have gap 0, so ties always share a session (order-free)
        while j < e.len() && ((e[j] >> 32) as u32) - prev <= gap {
            prev = (e[j] >> 32) as u32;
            if e[j] as u32 != 0 {
                pur.push(e[j]);
            }
            j += 1;
        }
        ses.push((start, prev, (j - i) as u32));
        i = j;
    }
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
    TL.store(std::env::var_os("TIMELINE").is_some(), Ordering::Relaxed);
    tlog("main entry");
    if std::env::var_os("WIDE").is_some() || !main_fast() {
        main_wide();
    }
}

fn main_fast() -> bool {
    T0.set(Instant::now()).ok();
    DIAGF.store(std::env::var_os("DIAG").is_some(), Ordering::Relaxed);
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
        .clamp(1, 256); // worker id fits the 8 bits it gets in a dedup locator

    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let w0 = manifest_ts(&man, "window_start");
    let w1 = manifest_ts(&man, "window_end");
    let day0 = w0.div_euclid(DAY);
    let ndays = ((w1 - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    // ---------------------------------------------------------- mmap
    // events and clicks are read with pread into per-worker buffers (no 1.8 GB of file PTEs to
    // populate and to zap at exit); MMAP=1 keeps the mapped reads (A/B knob)
    let rd = std::env::var_os("MMAP").is_none();
    FADV.store(std::env::var_os("FADV").is_some(), Ordering::Relaxed);
    let ev_in = open_inputs(&csv_files(&data.join("events")));
    let cl_in = open_inputs(&csv_files(&data.join("clicks")));
    let ev_files: Vec<S> = if rd { Vec::new() } else { csv_files(&data.join("events")).iter().filter_map(|p| map_file(p)).collect() };
    let cl_files: Vec<S> = if rd { Vec::new() } else { csv_files(&data.join("clicks")).iter().filter_map(|p| map_file(p)).collect() };
    let lk_files: Vec<S> = csv_files(&data.join("links")).iter().filter_map(|p| map_file(p)).collect();
    let total: usize = ev_in.iter().chain(cl_in.iter()).map(|d| d.len).sum();
    let mut np = 64usize;
    while np < 8192 && np * (6 << 20) < total {
        np *= 2;
    }
    if let Some(v) = std::env::var("NP").ok().and_then(|v| v.parse::<usize>().ok()).filter(|v| v.is_power_of_two()) {
        np = v; // knob for A/B only
    }
    let pmask = (np - 1) as u64;
    // user partitions for the ID->person join: each partition's slice of the link table should
    // stay cache resident; fewer partitions mean fewer scatter streams while parsing
    // np / 4: fewer scatter streams while parsing (NPU sweep 512/128/64/32 at 20M: 128 best, parse
    // -6%); each partition's link table (~2 x ids / npu x 16 B, ~0.3 MB at 20M) still fits a 1 MB L2
    let mut npu = (np / 4).max(64);
    if let Some(v) = std::env::var("NPU").ok().and_then(|v| v.parse::<usize>().ok()).filter(|v| v.is_power_of_two()) {
        npu = v; // knob for A/B only
    }
    let umask = (npu - 1) as u64;
    log(&format!("{} event, {} click, {} link files, {} MB, {} threads, {} partitions", ev_in.len(), cl_in.len(), lk_files.len(), total >> 20, nt, np));


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
    // static preconditions of the compact layout; otherwise the wide (a25) path runs
    let cw0_ = w0 - WEEK;
    if cats.len() > 256 || w1 - cw0_ >= (1i64 << 32) - 1 || w1 <= w0 {
        log("compact layout preconditions fail: wide path");
        return false;
    }
    // ---------------------------------------------------------- links (task 0 of the parse pool)
    // the link table is published through LINKS; phase 2 starts deduplicating without it
    // and only the person-map steps wait for it. It runs as the first task of the parse pool
    // (longest task first), so nt threads run on nt CPUs; LINKTHREAD=1 keeps the old extra thread.
    let publish_links = move |lk: &[S]| {
        let l = build_links(lk, npu);
        let _ = LINKS.set(l);
        let _g = LK_M.lock().unwrap();
        LK_CV.notify_all();
    };
    let lk_task = std::env::var_os("LINKTHREAD").is_none();
    let lt = lk_task as usize; // tasks before the first parse piece
    if !lk_task {
        let lk = lk_files.clone();
        std::thread::spawn(move || publish_links(&lk));
    }

    // ---------------------------------------------------------- parse events + clicks
    POPULATE.store(std::env::var_os("NOPOPULATE").is_none(), Ordering::Relaxed);
    if let Some(v) = std::env::var("PFD").ok().and_then(|v| v.parse::<usize>().ok()) {
        PFD.store(v, Ordering::Relaxed); // knob for A/B only
    }
    // pread pieces stay cache resident between the read and the parse (pod A/B CHK 256 vs 1024: no
    // detectable time difference; 1024 runs fewer instructions, so it stays the default)
    let ch = if rd { std::env::var("CHK").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(1024) << 10 } else { 4 << 20 };
    let ech = chunks_in(&ev_in, ch);
    let cch = chunks_in(&cl_in, ch);
    let ne = ech.len();
    struct P1 {
        w: u64,
        ar: Arena,
        dd: Vec<Bkt<DRec16>>, // dedup stream, by hash(event_id), npd partitions
        ddw: Vec<Bkt<DRec>>, // packed event_ids whose ingest / locator do not fit DRec16 (same partitions)
        ddl: Vec<Bkt<DRecL>>, // same for event_ids that do not pack (empty for generator data)
        pv: Vec<Bkt<PRec12>>, // valid copies, by hash(user key) partition (npu partitions)
        pp: Vec<Bkt<(u32, u32)>>, // purchases of pv: (index in pv[q], pc)
        cl: Vec<Bkt<CRec24>>, // valid clicks, by hash(user key) partition (npu partitions, as pv)
        dl: Vec<Line>, // write-combining staging lines of dd / pv
        pl: Vec<[Line; 3]>,
        dn: Vec<u32>, // record counts of dd / pv (hot; see push_wch)
        pn: Vec<u32>,
        buf: RdBuf,
        own: (*mut u8, usize), // bump space for bytes that must outlive a pread piece
        t_ev: f64,
        t_cl: f64,
    }
    unsafe impl Send for P1 {}
    unsafe impl Sync for P1 {}
    // dedup partitions: small enough that each partition's table stays in L2
    // capped at 2048 (60M: 4096 partitions would put 256 KB of staging lines in the parse loop)
    let mut npd = 256usize;
    while npd < 2048 && npd * (2 << 20) < total {
        npd *= 2;
    }
    if let Some(v) = std::env::var("ND").ok().and_then(|v| v.parse::<usize>().ok()).filter(|v| v.is_power_of_two()) {
        npd = v; // knob for A/B only
    }
    let dmask = (npd - 1) as u64;
    let (cw0, cw1) = (w0 - WEEK, w1);
    // window-restricted timestamp parsers (month prefix lookup instead of calendar math)
    let ev_win = Win::new(w0, w1);
    let cl_win = Win::new(cw0, cw1);
    // Generous per-(worker, partition) capacities: untouched reserved pages cost nothing,
    // and pre-sizing avoids realloc copies (and their page faults) while scattering.
    let ev_bytes: usize = ev_in.iter().map(|d| d.len).sum();
    let cl_bytes: usize = cl_in.iter().map(|d| d.len).sum();
    let reg1 = Region::new(ev_bytes / 40 * (std::mem::size_of::<DRec16>() + std::mem::size_of::<PRec12>()) + cl_bytes / 30 * std::mem::size_of::<CRec24>() + nt * ((npd + 2) * BLK_BYTES + SLAB));
    // DRec16 locator: worker (wb bits) | user partition (qb bits) | index (ib bits); copies whose
    // index does not fit, or whose ingest is not a whole second near the window, use DRec
    let wb = (usize::BITS - (nt - 1).leading_zeros()) as u64;
    let qb = npu.trailing_zeros() as u64;
    let ib = 32u64.saturating_sub(wb + qb);
    let mut ibcap: u64 = if ib >= 8 { (1u64 << ib) - 1 } else { 0 };
    if let Some(v) = std::env::var("IBCAP").ok().and_then(|v| v.parse::<u64>().ok()) {
        ibcap = ibcap.min(v); // test knob: forces valid copies with index >= v onto the wide dedup stream
    }
    let ibase: i64 = w0.div_euclid(1000) - (1i64 << 31);
    let ra: usize = std::env::var("RA").ok().and_then(|v| v.parse().ok()).unwrap_or(nt);
    #[inline(always)]
    fn idk_rd(rd: bool, s: S, lim: usize) -> K {
        if rd { idkf_own(s, lim) } else { idkf(s, lim) }
    }
    let idk_in = |s: S, lim: usize| idk_rd(rd, s, lim);
    let p1 = par(
        nt,
        ne + cch.len() + lt,
        |w| P1 {
            w: w as u64,
            ar: Arena::new(reg1),
            dd: (0..npd).map(|_| Bkt::new()).collect(),
            ddw: (0..npd).map(|_| Bkt::new()).collect(),
            ddl: (0..npd).map(|_| Bkt::new()).collect(),
            pv: (0..npu).map(|_| Bkt::new()).collect(),
            pp: (0..npu).map(|_| Bkt::new()).collect(),
            cl: (0..npu).map(|_| Bkt::new()).collect(),
            dl: vec![Line([0; 64]); npd],
            pl: vec![[Line([0; 64]); 3]; npu],
            dn: vec![0; npd],
            pn: vec![0; npu],
            buf: RdBuf::new(if rd { ch + (64 << 10) } else { 0 }),
            own: (std::ptr::null_mut(), 0),
            t_ev: 0.0,
            t_cl: 0.0,
        },
        |st, t| {
            if t < lt {
                publish_links(&lk_files);
                return;
            }
            let t = t - lt;
            let tt = tcpu();
            // keep `ra` pieces of reads in flight ahead of the parse frontier (cold page cache)
            if rd && ra > 0 {
                let u = t + ra;
                let (f, s, e) = if u < ne { let (fi, s, e) = ech[u]; (&ev_in[fi], s, e) } else if u < ne + cch.len() { let (fi, s, e) = cch[u - ne]; (&cl_in[fi], s, e) } else { (&ev_in[0], 0, 0) };
                if e > s {
                    readahead(f, s, e);
                }
            }
            if t < ne {
                let (fi, s, e) = ech[t];
                let (d, s, e, lim) = if rd {
                    st.buf.piece(&ev_in[fi], s, e)
                } else {
                    let d = ev_files[fi];
                    populate(d, s, e);
                    (d, s, e, d.as_ptr() as usize + d.len())
                };
                // worker bits of the locators, hoisted out of the row loop (st is written per row)
                let (wl, w32) = (st.w << 56, st.w << (qb + ib));
                for_rows::<7>(d, s, e, |f| {
                    if f[0].is_empty() {
                        return;
                    }
                    let (ing, iwhole, isec0) = match &cl_win {
                        Some(w) => ingest_win(f[3], w),
                        None => {
                            let ms = ingest_ms(f[3]); // R-TS.ING: always valid F1
                            (ms, ms.rem_euclid(1000) == 0, ms.div_euclid(1000))
                        }
                    };
                    let mut ts = BAD;
                    let mut pc = 0u32;
                    let mut ukey: K = 0;
                    'v: {
                        if f[1].is_empty() || f[5].is_empty() {
                            break 'v;
                        }
                        let t = match match &ev_win {
                            Some(w) => parse_ts_win(f[2], w),
                            None => parse_ts_fast(f[2]),
                        } {
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
                            let ph = hs(f[5]);
                            let cat = match prod_tab.find(ph, |e| e.0 == f[5]) {
                                Some(e) => e.1,
                                None => unknown_rank,
                            };
                            pc = ((v as u32) << 8) | cat;
                        } else if et != b"view" && et != b"cart" {
                            break 'v;
                        }
                        if ing - t > DAY {
                            break 'v;
                        }
                        ts = t;
                        ukey = idk_in(f[1], lim);
                    }
                    let _ = lim;
                    let (loc, l32) = if ts != BAD {
                        let q = ((ph(ukey) >> 40) & umask) as usize;
                        // q, b below are masked partition indexes (< the vector lengths)
                        let idx = unsafe { *st.pn.get_unchecked(q) } as u64;
                        let l = wl | ((q as u64) << 40) | idx;
                        if pc != 0 {
                            st.pp[q].push(&mut st.ar, (idx as u32, pc));
                        }
                        unsafe { st.pv.get_unchecked_mut(q).push_wc12(st.pl.get_unchecked_mut(q), st.pn.get_unchecked_mut(q), &mut st.ar, PRec12 { pk: ukey, ts: (ts - cw0) as u32 }) };
                        (l, if idx < ibcap { (w32 | ((q as u64) << ib) | idx) as u32 } else { NOLOC32 })
                    } else {
                        (NOLOC, NOLOC32)
                    };
                    match kfast(f[0], lim) {
                        Some(key) => {
                            let h = ph(key);
                            let isec = isec0 - ibase;
                            if iwhole && (0..u32::MAX as i64).contains(&isec) && (loc == NOLOC || l32 != NOLOC32) {
                                let b = ((h >> 40) & dmask) as usize;
                                unsafe { st.dd.get_unchecked_mut(b).push_wch(st.dl.get_unchecked_mut(b), st.dn.get_unchecked_mut(b), &mut st.ar, DRec16 { key, ing: isec as u32, loc: l32 }) };
                            } else {
                                st.ddw[((h >> 40) & dmask) as usize].push(&mut st.ar, DRec { key, ing, loc });
                            }
                        }
                        None => {
                            let h = hs(f[0]) | 1;
                            let ptr = if rd {
                                // the piece buffer is reused: keep a copy of the id bytes
                                let n = f[0].len();
                                if st.own.1 < n {
                                    let sz = n.max(1 << 20);
                                    st.own = (Box::leak(vec![0u8; sz].into_boxed_slice()).as_mut_ptr(), sz);
                                }
                                let p = st.own.0;
                                unsafe {
                                    std::ptr::copy_nonoverlapping(f[0].as_ptr(), p, n);
                                    st.own = (p.add(n), st.own.1 - n);
                                }
                                p as usize
                            } else {
                                f[0].as_ptr() as usize
                            };
                            st.ddl[((h >> 40) & dmask) as usize].push(&mut st.ar, DRecL { h, ing, loc, ptr, len: f[0].len() });
                        }
                    }
                });
                sfence();
                st.t_ev += tcpu() - tt;
            } else {
                let (fi, s, e) = cch[t - ne];
                let (d, s, e, lim) = if rd {
                    st.buf.piece(&cl_in[fi], s, e)
                } else {
                    let d = cl_files[fi];
                    populate(d, s, e);
                    (d, s, e, d.as_ptr() as usize + d.len())
                };
                for_rows::<4>(d, s, e, |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let ts = match match &cl_win {
                        Some(w) => parse_ts_win(f[3], w),
                        None => parse_ts_fast(f[3]),
                    } {
                        Some(t) if t >= cw0 && t < cw1 => t,
                        _ => return,
                    };
                    let camp = match camp_tab.find(hs(f[2]), |&i| camp_ids[i as usize] == f[2]) {
                        Some(&i) => i,
                        None => return,
                    };
                    let pk = idk_in(f[1], lim);
                    let q = ((ph(pk) >> 40) & umask) as usize;
                    st.cl[q].push(&mut st.ar, CRec24 { pk, cid: idk_in(f[0], lim), ts: (ts - cw0) as u32, camp });
                });
                st.t_cl += tcpu() - tt;
            }
        },
    );
    let mut p1 = p1;
    for s in p1.iter_mut() {
        let P1 { ar, dd, pv, dl, pl, dn, pn, .. } = s;
        for ((b, l), &n) in dd.iter_mut().zip(dl.iter()).zip(dn.iter()) {
            b.wc_flush_h(l, n, ar);
        }
        for ((b, l), &n) in pv.iter_mut().zip(pl.iter()).zip(pn.iter()) {
            b.wc_flush12(l, n, ar);
        }
    }
    let p1 = p1;
    {
        let ne: usize = p1.iter().map(|s| (s.dd.iter().map(|v| v.len()).sum::<usize>() + s.ddw.iter().map(|v| v.len()).sum::<usize>() + s.ddl.iter().map(|v| v.len()).sum::<usize>())).sum();
        let nc: usize = p1.iter().map(|s| s.cl.iter().map(|v| v.len()).sum::<usize>()).sum();
        let te: f64 = p1.iter().map(|s| s.t_ev).sum();
        let tc: f64 = p1.iter().map(|s| s.t_cl).sum();
        log(&format!("parsed: {} event copies ({:.2} thread-s), {} valid clicks ({:.2} thread-s)", ne, te, nc, tc));
    }

    // ---------------------------------------------------------- dedup (kill list), map persons
    // Phase 2a: per dedup partition, keep the min-ingest copy of each event_id and set the
    // kill bit of every other copy that has a valid-copy record. Click tasks (person map +
    // scatter) run in the same pass. Phase 2b: stream the valid copies, skip killed ones, map
    // to persons and scatter by hash(person).
    // global index of valid copy (worker w, user partition q, index i) = off[w * np + q] + i
    let mut off: Vec<usize> = vec![0; p1.len() * npu + 1];
    for (w, s) in p1.iter().enumerate() {
        for q in 0..npu {
            off[w * npu + q + 1] = off[w * npu + q] + s.pv[q].len();
        }
    }
    let nvalid = off[p1.len() * npu];
    let kill: Vec<std::sync::atomic::AtomicU64> = (0..(nvalid + 63) / 64).map(|_| std::sync::atomic::AtomicU64::new(0)).collect();
    let kill_loc = |loc: u64| {
        if loc != NOLOC {
            let g = off[(loc >> 56) as usize * npu + ((loc >> 40) & 0xFFFF) as usize] + (loc & ((1 << 40) - 1)) as usize;
            kill[g >> 6].fetch_or(1 << (g & 63), Ordering::Relaxed);
        }
    };
    struct P2 {
        ar: Arena,
        ev: Vec<Bkt<PRec16>>,
        cl: Vec<Bkt<CRec24>>,
        tab: Vec<DRec>,
        tab16: Vec<DRec16>,
        el: Vec<Line>,
        en: Vec<u32>,
        pur: Vec<(u32, u32)>,
        kills: usize,
        winners: usize,
        valid_winners: usize,
        t_dd: f64,
        t_map: f64,
        t_cl: f64,
    }
    let new_p2 = |a: &'static Region| P2 {
        ar: Arena::new(a),
        ev: (0..np).map(|_| Bkt::new()).collect(),
        cl: (0..np).map(|_| Bkt::new()).collect(),
        tab: Vec::new(),
        tab16: Vec::new(),
        el: Vec::new(),
        en: Vec::new(),
        pur: Vec::new(),
        kills: 0,
        winners: 0,
        valid_winners: 0,
        t_dd: 0.0,
        t_map: 0.0,
        t_cl: 0.0,
    };
    let reg2c = Region::new(64 << 20); // 2a pushes nothing; its arenas only collect consumed blocks
    let mut p2c = par(
        nt,
        npd,
        |_| new_p2(reg2c),
        |st, p| {
            let tt = tcpu();
            if p < npd {
                let (mut kills, mut winners, mut valid_winners) = (0usize, 0usize, 0usize);
                let ilm = (1u64 << ib) - 1;
                let qm = (1u64 << qb) - 1;
                let wloc = |l: u32| if l == NOLOC32 { NOLOC } else {
                    let l = l as u64;
                    ((l >> (qb + ib)) << 56) | (((l >> ib) & qm) << 40) | (l & ilm)
                };
                let wide = |r: &DRec16| DRec { key: r.key, ing: (r.ing as i64 + ibase) * 1000, loc: wloc(r.loc) };
                let capw: usize = p1.iter().map(|s| s.ddw[p].len()).sum();
                if capw == 0 {
                    // every copy of this partition is compact: a 16-byte-slot table (ingest
                    // seconds order the same way as ms, since all of them are whole seconds)
                    // power of two >= 2x the copies (pod A/B: 1.5x is slower, dedup 0.29 -> 0.36 thread-s)
                    let cap: usize = p1.iter().map(|s| s.dd[p].len()).sum();
                    let mut sz = 16usize;
                    while sz < cap * 2 {
                        sz *= 2;
                    }
                    let mask = sz - 1;
                    st.tab16.clear();
                    st.tab16.resize(sz, DRec16 { key: 0, ing: 0, loc: 0 });
                    let tab = &mut st.tab16;
                    for s in p1.iter() {
                        for r in s.dd[p].iter() {
                            let mut i = ht(r.key) as usize & mask;
                            loop {
                                let e = unsafe { tab.get_unchecked_mut(i) }; // i <= mask < len
                                if e.key == r.key {
                                    if r.ing < e.ing {
                                        kills += (e.loc != NOLOC32) as usize;
                                        kill_loc(wloc(e.loc));
                                        valid_winners -= (e.loc != NOLOC32) as usize;
                                        valid_winners += (r.loc != NOLOC32) as usize;
                                        *e = *r;
                                    } else {
                                        kills += (r.loc != NOLOC32) as usize;
                                        kill_loc(wloc(r.loc));
                                    }
                                    break;
                                }
                                if e.key == 0 {
                                    *e = *r;
                                    winners += 1;
                                    valid_winners += (r.loc != NOLOC32) as usize;
                                    break;
                                }
                                i = (i + 1) & mask;
                            }
                        }
                    }
                } else {
                let cap: usize = p1.iter().map(|s| s.dd[p].len() + s.ddw[p].len()).sum();
                let mut sz = 16usize;
                while sz < cap * 2 {
                    sz *= 2;
                }
                let mask = sz - 1;
                st.tab.clear();
                st.tab.resize(sz, DRec { key: 0, ing: 0, loc: 0 }); // key 0 = empty slot (empty event_ids are dropped)
                let tab = &mut st.tab;
                for s in p1.iter() {
                    for r in s.dd[p].iter().map(wide).chain(s.ddw[p].iter().copied()) {
                        let r = &r;
                        let mut i = (h128(r.key) >> 8) as usize & mask;
                        loop {
                            let e = &mut tab[i];
                            if e.key == r.key {
                                // smallest ingest wins; equal ingests are identical copies (keep the first)
                                if r.ing < e.ing {
                                    kills += (e.loc != NOLOC) as usize;
                                    kill_loc(e.loc);
                                    valid_winners -= (e.loc != NOLOC) as usize;
                                    valid_winners += (r.loc != NOLOC) as usize;
                                    *e = *r;
                                } else {
                                    kills += (r.loc != NOLOC) as usize;
                                    kill_loc(r.loc);
                                }
                                break;
                            }
                            if e.key == 0 {
                                *e = *r;
                                winners += 1;
                                valid_winners += (r.loc != NOLOC) as usize;
                                break;
                            }
                            i = (i + 1) & mask;
                        }
                    }
                }
                }
                // event_ids that do not pack: same rule, hash match confirmed byte for byte
                let capl: usize = p1.iter().map(|s| s.ddl[p].len()).sum();
                if capl > 0 {
                    let mut sz = 16usize;
                    while sz < capl * 2 {
                        sz *= 2;
                    }
                    let mask = sz - 1;
                    let bytes = |r: &DRecL| unsafe { std::slice::from_raw_parts(r.ptr as *const u8, r.len) };
                    let mut tl: Vec<DRecL> = vec![DRecL { h: 0, ing: 0, loc: 0, ptr: 0, len: 0 }; sz];
                    for s in p1.iter() {
                        for r in s.ddl[p].iter() {
                            let mut i = (r.h >> 8) as usize & mask;
                            loop {
                                let e = &mut tl[i];
                                if e.h == r.h && bytes(e) == bytes(r) {
                                    if r.ing < e.ing {
                                        kills += (e.loc != NOLOC) as usize;
                                        kill_loc(e.loc);
                                        valid_winners -= (e.loc != NOLOC) as usize;
                                        valid_winners += (r.loc != NOLOC) as usize;
                                        *e = *r;
                                    } else {
                                        kills += (r.loc != NOLOC) as usize;
                                        kill_loc(r.loc);
                                    }
                                    break;
                                }
                                if e.h == 0 {
                                    *e = *r;
                                    winners += 1;
                                    valid_winners += (r.loc != NOLOC) as usize;
                                    break;
                                }
                                i = (i + 1) & mask;
                            }
                        }
                    }
                    for s in p1.iter() {
                        s.ddl[p].recycle_into(&mut st.ar);
                    }
                }
                st.kills += kills;
                st.winners += winners;
                st.valid_winners += valid_winners;
                // bucket p of every worker is consumed: reuse its blocks for this worker's pushes
                for s in p1.iter() {
                    s.dd[p].recycle_into(&mut st.ar);
                    s.ddw[p].recycle_into(&mut st.ar);
                }
                st.t_dd += tcpu() - tt;
            }
        },
    );
    if DIAGF.load(Ordering::Relaxed) { log("phase 2a done (dedup)"); } else { tlog("phase 2a done (dedup)"); }
    // phase 2b: per user partition, join its valid copies against that partition's links
    // (a small table, cache resident), skip killed copies, scatter by hash(person)
    let per_pv = Bkt::<PRec12>::PER;
    let reg2 = Region::new(ev_bytes / 40 * std::mem::size_of::<PRec16>() + cl_bytes / 30 * std::mem::size_of::<CRec24>() + nt * ((2 * np + 2) * BLK_BYTES + SLAB));
    // the dedup blocks consumed in phase 2a are already faulted in: phase 2b writes into them first
    let pool: std::sync::Mutex<Vec<Vec<usize>>> = std::sync::Mutex::new(p2c.iter_mut().map(|s| std::mem::take(&mut s.ar.free).into_iter().map(|p| p as usize).collect()).collect());
    let p2 = par(
        nt,
        npu,
        |_| {
            let mut s = new_p2(reg2);
            s.el = vec![Line([0; 64]); np];
            s.en = vec![0; np];
            s.ar.free = pool.lock().unwrap().pop().unwrap_or_default().into_iter().map(|p| p as *mut u8).collect();
            s
        },
        |st, q| {
            let tt = tcpu();
            let links = links_wait();
            let lp = &links.parts[q];
            // Only remapped ids (person != id) are stored: a missing id is its own person.
            // 4 (id, person) pairs per cache line, buckets >= remapped ids (mean load <= 1 of 4),
            // so a probe almost always ends in its first line (one predictable loop exit), and
            // hit/miss (data-random: ~40% of rows have a remapped user) is a mask select, not a
            // branch. The linear-probe slot table this replaces cost ~1 mispredict per lookup.
            let nrm = lp.iter().filter(|e| e.0 != e.1).count();
            let mut nb = 1usize;
            while nb < nrm {
                nb *= 2;
            }
            let bmask = nb - 1;
            let mut lt: Vec<KB8> = vec![KB8([0; 8]); nb];
            for &(k, v) in lp {
                if k == v {
                    continue;
                }
                let mut b = ht(k) as usize & bmask;
                loop {
                    if let Some(j) = (0..4).find(|&j| lt[b].0[2 * j] == 0) {
                        lt[b].0[2 * j] = k;
                        lt[b].0[2 * j + 1] = v;
                        break;
                    }
                    b = (b + 1) & bmask;
                }
            }
            let person = |k: K| -> K {
                let mut b = ht(k) as usize & bmask;
                loop {
                    let e = unsafe { lt.get_unchecked(b) };
                    let (hm, zm) = kb8_pairs(e, k);
                    // value lane after the first matching key lane (lane 7 if none: masked off)
                    let v = unsafe { *e.0.get_unchecked(((hm | 0x40).trailing_zeros() + 1) as usize) };
                    let r = sel_nz(hm, v, k);
                    // the key is absent once a line has an empty pair (insertion fills lines in order)
                    if (hm | zm) != 0 {
                        return r;
                    }
                    b = (b + 1) & bmask;
                }
            };
            for (w, s) in p1.iter().enumerate() {
                let o = off[w * npu + q];
                // purchases of this stream in index order, then a sentinel (no index matches it)
                let pur = &mut st.pur;
                pur.clear();
                pur.extend(s.pp[q].iter().copied());
                pur.push((u32::MAX, 0));
                let mut pi = 0usize;
                let mut li = 0u32;
                for (bi, sl) in s.pv[q].slices().enumerate() {
                    let base = o + bi * per_pv;
                    for (j, r) in sl.iter().enumerate() {
                        let np = unsafe { *pur.get_unchecked(pi) };
                        let hit = np.0 == li;
                        let pc = if hit { np.1 } else { 0 };
                        pi += hit as usize;
                        li += 1;
                        let g = base + j;
                        if (unsafe { kill.get_unchecked(g >> 6) }.load(Ordering::Relaxed) >> (g & 63)) & 1 != 0 {
                            continue;
                        }
                        let (rpk, rts) = (r.pk, r.ts);
                        let pk = person(rpk);
                        let b = ((ph(pk) >> 40) & pmask) as usize;
                        unsafe { st.ev.get_unchecked_mut(b).push_wch(st.el.get_unchecked_mut(b), st.en.get_unchecked_mut(b), &mut st.ar, PRec16 { pk, ts: rts, pc }) };
                    }
                }
                s.pv[q].recycle_into(&mut st.ar);
                s.pp[q].recycle_into(&mut st.ar);
            }
            sfence();
            let t1 = tcpu();
            st.t_map += t1 - tt;
            for s in p1.iter() {
                for r in s.cl[q].iter() {
                    let pk = person(r.pk);
                    st.cl[((ph(pk) >> 40) & pmask) as usize].push(&mut st.ar, CRec24 { pk, ..*r });
                }
                s.cl[q].recycle_into(&mut st.ar);
            }
            st.t_cl += tcpu() - t1;
        },
    );
    let mut p2 = p2;
    for s in p2.iter_mut() {
        let P2 { ar, ev, el, en, .. } = s;
        for ((b, l), &n) in ev.iter_mut().zip(el.iter()).zip(en.iter()) {
            b.wc_flush_h(l, n, ar);
        }
    }
    let p2 = p2;
    {
        let n: usize = p2.iter().map(|s| s.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
        let a: f64 = p2c.iter().map(|s| s.t_dd).sum();
        let b: f64 = p2.iter().map(|s| s.t_map).sum();
        let c: f64 = p2.iter().map(|s| s.t_cl).sum();
        let kills: usize = p2c.iter().map(|s| s.kills).sum();
        let winners: usize = p2c.iter().map(|s| s.winners).sum();
        let vw: usize = p2c.iter().map(|s| s.valid_winners).sum();
        let copies: usize = p1.iter().map(|s| (s.dd.iter().map(|v| v.len()).sum::<usize>() + s.ddw.iter().map(|v| v.len()).sum::<usize>() + s.ddl.iter().map(|v| v.len()).sum::<usize>())).sum();
        // invariants: every valid copy is either the kept winner or killed; every copy is a winner or a loser
        let ok = nvalid == vw + kills && n == vw && copies >= winners;
        log(&format!(
            "dedup: {} surviving events (thread-s: dedup {:.2}, event person map {:.2}, click person map {:.2}); copies {} winners {} valid winners {} kills {} valid copies {} invariant {}",
            n, a, b, c, copies, winners, vw, kills, nvalid, if ok { "OK" } else { "FAIL" }
        ));
        log(&format!("links: {} ids", links_wait().n));
    }

    freeze_long();

    // ---------------------------------------------------------- per person
    // Events are u64 = (ts offset << 32) | pc, so a plain u64 sort orders them by ts; the
    // per-partition buffers live in the worker state and are reused (no fresh pages per task).
    struct P3 {
        rows: URows,
        acc: Vec<[i64; 4]>, // per (campaign incl. organic, day): sessions, purchasing, revenue, assisted
        evs: Vec<u64>,
        cks: Vec<Ck16>,
        pids: Vec<u32>,
        names: Vec<K>,
        cnt: Vec<u32>,
        off: Vec<usize>,
        pos: Vec<usize>,
        coff: Vec<usize>,
        cpos: Vec<usize>,
        tab: Itab,
        vals: Vec<u64>,
        cslot: Vec<u32>, // campaign -> open island index while a person's assists are built (u32::MAX: none)
        bk: Bk,
        ses: Vec<(u32, u32, u32)>,
        pur: Vec<u64>,
        t_grp: f64,
        t_sort: f64,
        t_sess: f64,
        tk: Vec<(u32, u32, f64, f64)>, // DIAG: (partition, events, start s, dur s) per task
    }
    let organic = ncamp as u32;
    let p3sort = std::env::var_os("P3SORT").is_some_and(|v| v != "0");
    let nsurv: usize = p2.iter().map(|s| s.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
    // largest partitions first (LPT): every partition's size is known after the scatter, so one
    // giant person's partition starts at once instead of straggling at the end of the phase
    let psz: Vec<usize> = (0..np).map(|q| p2.iter().map(|s| s.ev[q].len() + s.cl[q].len() / 2).sum()).collect();
    let mut order: Vec<u32> = (0..np as u32).collect();
    if std::env::var("LPT").map(|v| v != "0").unwrap_or(true) {
        order.sort_by(|&a, &b| psz[b as usize].cmp(&psz[a as usize]).then(a.cmp(&b)));
    }
    let diag = DIAGF.load(Ordering::Relaxed);
    let p3t0 = Instant::now();
    let p3 = par(
        nt,
        np,
        |_| P3 {
            rows: {
                // reserved, not touched: no realloc copies (and their fresh pages) while rows grow
                let c = nsurv / (nt * 4) + 1024;
                URows { name: Vec::with_capacity(c), day: Vec::with_capacity(c), ses: Vec::with_capacity(c), ev: Vec::with_capacity(c), rev: Vec::with_capacity(c), top: Vec::with_capacity(c) }
            },
            tab: Itab::new(),
            vals: Vec::new(),
            acc: vec![[0i64; 4]; (ncamp + 1) * ndays],
            evs: Vec::new(),
            cks: Vec::new(),
            pids: Vec::new(),
            names: Vec::new(),
            cnt: Vec::new(),
            off: Vec::new(),
            pos: Vec::new(),
            coff: Vec::new(),
            cpos: Vec::new(),
            cslot: vec![u32::MAX; ncamp + 1],
            bk: Bk::new(),
            ses: Vec::new(),
            pur: Vec::new(),
            t_grp: 0.0,
            t_sort: 0.0,
            t_sess: 0.0,
            tk: Vec::new(),
        },
        |st, t| {
            let q = order[t] as usize;
            let w0i = if diag { p3t0.elapsed().as_secs_f64() } else { 0.0 };
            let tt = tcpu();
            let cap: usize = p2.iter().map(|s| s.ev[q].len()).sum();
            let tab = &mut st.tab;
            tab.reset(cap / 16 + 16);
            let names = &mut st.names;
            let cnt = &mut st.cnt;
            let pids = &mut st.pids;
            let vals = &mut st.vals;
            names.clear();
            cnt.clear();
            pids.clear();
            vals.clear();
            // one pass over the partition's records: pid and the event value are staged in
            // worker buffers, so the scatter does not read the records again
            for s in p2.iter() {
                for r in s.ev[q].iter() {
                    let pid = tab.intern(r.pk, names);
                    if pid as usize == cnt.len() {
                        cnt.push(0);
                    }
                    cnt[pid as usize] += 1;
                    pids.push(pid);
                    vals.push(((r.ts as u64) << 32) | r.pc as u64);
                }
            }
            let npers = names.len();
            if npers == 0 {
                return;
            }
            let off = &mut st.off;
            off.clear();
            off.push(0);
            for i in 0..npers {
                let x = off[i] + cnt[i] as usize;
                off.push(x);
            }
            let pos = &mut st.pos;
            pos.clear();
            pos.extend_from_slice(&off[..npers]);
            let evs = &mut st.evs;
            evs.clear();
            evs.reserve(cap);
            // every slot 0..cap is written exactly once by the scatter below
            unsafe { evs.set_len(cap) };
            for (&pid, &v) in pids.iter().zip(vals.iter()) {
                // pid < npers = pos.len(), and pos[pid] < off[pid + 1] <= cap = evs.len()
                unsafe {
                    let p = pos.get_unchecked_mut(pid as usize);
                    *evs.get_unchecked_mut(*p) = v;
                    *p += 1;
                }
            }
            // clicks of persons with events (pids reused for the click pass)
            let ccnt = cnt;
            ccnt.clear();
            ccnt.resize(npers, 0);
            let cp = pids;
            cp.clear();
            for s in p2.iter() {
                for r in s.cl[q].iter() {
                    let pid = tab.find(r.pk);
                    if pid != u32::MAX {
                        ccnt[pid as usize] += 1;
                    }
                    cp.push(pid);
                }
            }
            let coff = &mut st.coff;
            coff.clear();
            coff.push(0);
            for i in 0..npers {
                let x = coff[i] + ccnt[i] as usize;
                coff.push(x);
            }
            let cpos = &mut st.cpos;
            cpos.clear();
            cpos.extend_from_slice(&coff[..npers]);
            let cks = &mut st.cks;
            cks.clear();
            cks.reserve(coff[npers]);
            unsafe { cks.set_len(coff[npers]) };
            let mut k = 0;
            for s in p2.iter() {
                for r in s.cl[q].iter() {
                    let pid = cp[k];
                    k += 1;
                    if pid != u32::MAX {
                        cks[cpos[pid as usize]] = Ck16 { cid: r.cid, ts: r.ts, camp: r.camp };
                        cpos[pid as usize] += 1;
                    }
                }
            }
            let t1 = tcpu();
            st.t_grp += t1 - tt;
            for pid in 0..npers {
                cks[coff[pid]..coff[pid + 1]].sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| id_cmp(a.cid, b.cid)));
            }
            let t2 = tcpu();
            st.t_sort += t2 - t1;

            let gap = GAP as u32;
            let week = WEEK as u32;
            let mut starts: Vec<i64> = Vec::new();
            let mut sdays: Vec<u32> = Vec::new(); // day index of each session start
            let mut catsum: Vec<(u32, i64)> = Vec::new();
            let mut open: Vec<(u32, u32, u32, u32)> = Vec::new(); // (campaign, island start, island end, pad)
            let mut closed: Vec<(u32, u32, u32, u32)> = Vec::new();
            let ses = &mut st.ses;
            let pur = &mut st.pur;
            for pid in 0..npers {
                if p3sort {
                    sessions_sort(&mut evs[off[pid]..off[pid + 1]], ses, pur);
                } else {
                    sessions_bucket(&evs[off[pid]..off[pid + 1]], &mut st.bk, ses, pur);
                }
                let c = &cks[coff[pid]..coff[pid + 1]];
                let name = names[pid];
                starts.clear();
                sdays.clear();
                // user_daily accumulators for the current day
                let mut cur_day = i64::MIN;
                let (mut d_ses, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
                catsum.clear();
                let mut hi = 0usize;
                let mut pi = 0usize;
                for &(start32, end32, nev) in ses.iter() {
                    let start = start32 as i64 + cw0;
                    let mut rev = 0i64;
                    let mut purch = false;
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
                    // the session's purchases: the next ones in ts order up to its end
                    while pi < pur.len() && ((pur[pi] >> 32) as u32) <= end32 {
                        let pc = pur[pi] as u32;
                        pi += 1;
                        let (price, cat) = ((pc >> 8) as i64, pc & 0xFF);
                        purch = true;
                        rev += price;
                        match catsum.iter_mut().find(|c| c.0 == cat) {
                            Some(c) => c.1 += price,
                            None => catsum.push((cat, price)),
                        }
                    }
                    d_ses += 1;
                    d_ev += nev as i64;
                    d_rev += rev;
                    starts.push(start);
                    sdays.push((day - day0) as u32);
                    // credit: latest click with ts <= start (clicks sorted by ts, then click_id)
                    while hi < c.len() && c[hi].ts <= start32 {
                        hi += 1;
                    }
                    let mut camp = organic;
                    if hi > 0 {
                        let t = c[hi - 1].ts;
                        if start32 - t <= week {
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
                }
                if cur_day != i64::MIN {
                    flush(&mut st.rows, name, cur_day, d_ses, d_ev, d_rev, &catsum);
                }
                // assists: per campaign, merge [click, click + 7d] into islands and count the
                // session starts inside each island, per day (binary search on sorted starts)
                // Clicks are already in ts order, so each campaign's islands are built in one
                // pass with a campaign -> open-island slot table (no per-person sort).
                if !c.is_empty() {
                    open.clear();
                    for x in c {
                        let s = st.cslot[x.camp as usize];
                        if s == u32::MAX {
                            st.cslot[x.camp as usize] = open.len() as u32;
                            open.push((x.camp, x.ts, x.ts, 0));
                        } else {
                            let o = &mut open[s as usize];
                            if x.ts - o.2 <= week {
                                o.2 = x.ts;
                            } else {
                                // island [o.1, o.2] is closed: keep it, start a new one
                                let done = *o;
                                o.1 = x.ts;
                                o.2 = x.ts;
                                closed.push(done);
                            }
                        }
                    }
                    for o in open.iter() {
                        st.cslot[o.0 as usize] = u32::MAX;
                    }
                    closed.extend_from_slice(&open);
                    for &(camp, a32, b32, _) in closed.iter() {
                        let (a, b) = (a32 as i64 + cw0, b32 as i64 + cw0);
                        let lo = starts.partition_point(|&s| s < a);
                        let hi = starts.partition_point(|&s| s <= b + WEEK);
                        let row = &mut st.acc[camp as usize * ndays..(camp as usize + 1) * ndays];
                        for &d in &sdays[lo..hi] {
                            row[d as usize][3] += 1;
                        }
                    }
                    closed.clear();
                }
            }
            st.t_sess += tcpu() - t2;
            if diag {
                let w1i = p3t0.elapsed().as_secs_f64();
                st.tk.push((q as u32, cap as u32, w0i, w1i - w0i));
            }
        },
    );
    if diag {
        let wall = p3t0.elapsed().as_secs_f64();
        let mut all: Vec<(u32, u32, f64, f64)> = p3.iter().flat_map(|s| s.tk.iter().copied()).collect();
        all.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap());
        let sum: f64 = all.iter().map(|x| x.3).sum();
        let mean = sum / all.len().max(1) as f64;
        let fin: Vec<f64> = p3.iter().map(|s| s.tk.iter().map(|x| x.2 + x.3).fold(0.0, f64::max)).collect();
        let last = fin.iter().cloned().fold(0.0, f64::max);
        let idle: f64 = fin.iter().map(|f| last - f).sum();
        log(&format!("p3 diag: wall {:.1} ms, tasks {}, sum {:.1} ms, mean {:.3} ms, max {:.1} ms, idle at barrier {:.1} thread-ms", wall * 1e3, all.len(), sum * 1e3, mean * 1e3, all.first().map(|x| x.3).unwrap_or(0.0) * 1e3, idle * 1e3));
        for x in all.iter().take(8) {
            log(&format!("p3 task q {} events {} start {:.1} ms dur {:.1} ms", x.0, x.1, x.2 * 1e3, x.3 * 1e3));
        }
    }
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
    // narrowest integer width that holds every value of the table (same type in every file)
    let fits = |f: &dyn Fn(&URows) -> &Vec<i64>| p3.iter().all(|s| f(&s.rows).iter().all(|&x| x >= i32::MIN as i64 && x <= i32::MAX as i64));
    let w32 = [fits(&|r| &r.ses), fits(&|r| &r.ev), fits(&|r| &r.rev)];
    tlog("write start");
    std::thread::scope(|sc| {
        for (k, &i) in nonempty.iter().enumerate() {
            let r = &p3[i].rows;
            let ud = &ud;
            sc.spawn(move || write_user_raw(&ud.join(format!("part-{:05}.parquet", k)), r, cats, w32));
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
        if rows.is_empty() {
            write_campaign(&cd.join("part-00000.parquet"), &rows);
        } else {
            write_campaign_raw(&cd.join("part-00000.parquet"), &rows);
        }
        log(&format!("campaign_daily {} rows", rows.len()));
    });
    log(&format!("done: user_daily {} rows (zstd cpu {:.1} ms)", nrows, ZNS.load(Ordering::Relaxed) as f64 / 1e6));
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
    tlog("exit");
    std::process::exit(0);
}

fn main_wide() {
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
        .clamp(1, 256); // worker id fits the 8 bits it gets in a dedup locator

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
    // user partitions for the ID->person join: each partition's slice of the link table should
    // stay cache resident; fewer partitions mean fewer scatter streams while parsing
    // (knob race at load ~11: 128 vs 512 within noise, so it stays equal to np)
    let mut npu = np;
    if let Some(v) = std::env::var("NPU").ok().and_then(|v| v.parse::<usize>().ok()).filter(|v| v.is_power_of_two()) {
        npu = v; // knob for A/B only
    }
    let umask = (npu - 1) as u64;
    log(&format!("{} event, {} click, {} link files, {} MB, {} threads, {} partitions", ev_files.len(), cl_files.len(), lk_files.len(), total >> 20, nt, np));

    // ---------------------------------------------------------- links (own thread, overlaps parsing)
    // the link table is published through LINKS; phase 2 starts deduplicating without it
    // and only the person-map steps wait for it
    std::thread::spawn(move || {
        let l = build_links(&lk_files, npu);
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
    POPULATE.store(std::env::var_os("NOPOPULATE").is_none(), Ordering::Relaxed);
    let ech = chunks(&ev_files, 4 << 20);
    let cch = chunks(&cl_files, 4 << 20);
    let ne = ech.len();
    struct P1 {
        w: u64,
        ar: Arena,
        dd: Vec<Bkt<DRec>>, // dedup stream, by hash(event_id), npd partitions
        ddl: Vec<Bkt<DRecL>>, // same for event_ids that do not pack (empty for generator data)
        pv: Vec<Bkt<PRec>>, // valid copies, by hash(user key) partition (np partitions)
        cl: Bkt<CRaw>,
        t_ev: f64,
        t_cl: f64,
    }
    // dedup partitions: small enough that each partition's table stays in L2
    let mut npd = 256usize;
    while npd < 65536 && npd * (2 << 20) < total {
        npd *= 2;
    }
    if let Some(v) = std::env::var("ND").ok().and_then(|v| v.parse::<usize>().ok()).filter(|v| v.is_power_of_two()) {
        npd = v; // knob for A/B only
    }
    let dmask = (npd - 1) as u64;
    let (cw0, cw1) = (w0 - WEEK, w1);
    // Generous per-(worker, partition) capacities: untouched reserved pages cost nothing,
    // and pre-sizing avoids realloc copies (and their page faults) while scattering.
    let ev_bytes: usize = ev_files.iter().map(|d| d.len()).sum();
    let cl_bytes: usize = cl_files.iter().map(|d| d.len()).sum();
    let reg1 = Region::new(ev_bytes / 40 * (std::mem::size_of::<DRec>() + std::mem::size_of::<PRec>()) + cl_bytes / 30 * std::mem::size_of::<CRaw>() + nt * ((npd + 2) * BLK_BYTES + SLAB));
    let p1 = par(
        nt,
        ne + cch.len(),
        |w| P1 {
            w: w as u64,
            ar: Arena::new(reg1),
            dd: (0..npd).map(|_| Bkt::new()).collect(),
            ddl: (0..npd).map(|_| Bkt::new()).collect(),
            pv: (0..npu).map(|_| Bkt::new()).collect(),
            cl: Bkt::new(),
            t_ev: 0.0,
            t_cl: 0.0,
        },
        |st, t| {
            let tt = tcpu();
            if t < ne {
                let (d, s, e) = ech[t];
                populate(d, s, e);
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
                    let _ = lim;
                    let loc = if ts != BAD {
                        let q = ((ph(ukey) >> 40) & umask) as usize;
                        let l = (st.w << 56) | ((q as u64) << 40) | st.pv[q].len() as u64;
                        st.pv[q].push(&mut st.ar, PRec { pk: ukey, ts, price, cat });
                        l
                    } else {
                        NOLOC
                    };
                    match kfast_s(f[0]) {
                        Some(key) => {
                            let h = h128(key);
                            st.dd[((h >> 40) & dmask) as usize].push(&mut st.ar, DRec { key, ing, loc });
                        }
                        None => {
                            let h = hs(f[0]) | 1;
                            st.ddl[((h >> 40) & dmask) as usize].push(&mut st.ar, DRecL { h, ing, loc, ptr: f[0].as_ptr() as usize, len: f[0].len() });
                        }
                    }
                });
                st.t_ev += tcpu() - tt;
            } else {
                let (d, s, e) = cch[t - ne];
                populate(d, s, e);
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
        let ne: usize = p1.iter().map(|s| (s.dd.iter().map(|v| v.len()).sum::<usize>() + s.ddl.iter().map(|v| v.len()).sum::<usize>())).sum();
        let nc: usize = p1.iter().map(|s| s.cl.len()).sum();
        let te: f64 = p1.iter().map(|s| s.t_ev).sum();
        let tc: f64 = p1.iter().map(|s| s.t_cl).sum();
        log(&format!("parsed: {} event copies ({:.2} thread-s), {} valid clicks ({:.2} thread-s)", ne, te, nc, tc));
    }

    // ---------------------------------------------------------- dedup (kill list), map persons
    // Phase 2a: per dedup partition, keep the min-ingest copy of each event_id and set the
    // kill bit of every other copy that has a valid-copy record. Click tasks (person map +
    // scatter) run in the same pass. Phase 2b: stream the valid copies, skip killed ones, map
    // to persons and scatter by hash(person).
    const CSLB: usize = 64; // blocks per stream task
    let cl_slices: Vec<Vec<&[CRaw]>> = p1.iter().map(|s| s.cl.slices().collect()).collect();
    let mut ctasks: Vec<(usize, usize)> = Vec::new();
    for (w, s) in cl_slices.iter().enumerate() {
        let mut a = 0;
        while a < s.len() {
            ctasks.push((w, a));
            a += CSLB;
        }
    }
    // global index of valid copy (worker w, user partition q, index i) = off[w * np + q] + i
    let mut off: Vec<usize> = vec![0; p1.len() * npu + 1];
    for (w, s) in p1.iter().enumerate() {
        for q in 0..npu {
            off[w * npu + q + 1] = off[w * npu + q] + s.pv[q].len();
        }
    }
    let nvalid = off[p1.len() * npu];
    let kill: Vec<std::sync::atomic::AtomicU64> = (0..(nvalid + 63) / 64).map(|_| std::sync::atomic::AtomicU64::new(0)).collect();
    let kill_loc = |loc: u64| {
        if loc != NOLOC {
            let g = off[(loc >> 56) as usize * npu + ((loc >> 40) & 0xFFFF) as usize] + (loc & ((1 << 40) - 1)) as usize;
            kill[g >> 6].fetch_or(1 << (g & 63), Ordering::Relaxed);
        }
    };
    struct P2 {
        ar: Arena,
        ev: Vec<Bkt<PRec>>,
        cl: Vec<Bkt<CRec>>,
        tab: Vec<DRec>,
        kills: usize,
        winners: usize,
        valid_winners: usize,
        t_dd: f64,
        t_map: f64,
        t_cl: f64,
    }
    let new_p2 = |a: &'static Region| P2 {
        ar: Arena::new(a),
        ev: (0..np).map(|_| Bkt::new()).collect(),
        cl: (0..np).map(|_| Bkt::new()).collect(),
        tab: Vec::new(),
        kills: 0,
        winners: 0,
        valid_winners: 0,
        t_dd: 0.0,
        t_map: 0.0,
        t_cl: 0.0,
    };
    let reg2c = Region::new(cl_bytes / 30 * std::mem::size_of::<CRec>() + nt * ((np + 2) * BLK_BYTES + SLAB));
    let p2c = par(
        nt,
        npd + ctasks.len(),
        |_| new_p2(reg2c),
        |st, p| {
            let tt = tcpu();
            if p < npd {
                let cap: usize = p1.iter().map(|s| s.dd[p].len()).sum();
                let mut sz = 16usize;
                while sz < cap * 2 {
                    sz *= 2;
                }
                let mask = sz - 1;
                st.tab.clear();
                st.tab.resize(sz, DRec { key: 0, ing: 0, loc: 0 }); // key 0 = empty slot (empty event_ids are dropped)
                let tab = &mut st.tab;
                let (mut kills, mut winners, mut valid_winners) = (0usize, 0usize, 0usize);
                for s in p1.iter() {
                    for r in s.dd[p].iter() {
                        let mut i = (h128(r.key) >> 8) as usize & mask;
                        loop {
                            let e = &mut tab[i];
                            if e.key == r.key {
                                // smallest ingest wins; equal ingests are identical copies (keep the first)
                                if r.ing < e.ing {
                                    kills += (e.loc != NOLOC) as usize;
                                    kill_loc(e.loc);
                                    valid_winners -= (e.loc != NOLOC) as usize;
                                    valid_winners += (r.loc != NOLOC) as usize;
                                    *e = *r;
                                } else {
                                    kills += (r.loc != NOLOC) as usize;
                                    kill_loc(r.loc);
                                }
                                break;
                            }
                            if e.key == 0 {
                                *e = *r;
                                winners += 1;
                                valid_winners += (r.loc != NOLOC) as usize;
                                break;
                            }
                            i = (i + 1) & mask;
                        }
                    }
                }
                // event_ids that do not pack: same rule, hash match confirmed byte for byte
                let capl: usize = p1.iter().map(|s| s.ddl[p].len()).sum();
                if capl > 0 {
                    let mut sz = 16usize;
                    while sz < capl * 2 {
                        sz *= 2;
                    }
                    let mask = sz - 1;
                    let bytes = |r: &DRecL| unsafe { std::slice::from_raw_parts(r.ptr as *const u8, r.len) };
                    let mut tl: Vec<DRecL> = vec![DRecL { h: 0, ing: 0, loc: 0, ptr: 0, len: 0 }; sz];
                    for s in p1.iter() {
                        for r in s.ddl[p].iter() {
                            let mut i = (r.h >> 8) as usize & mask;
                            loop {
                                let e = &mut tl[i];
                                if e.h == r.h && bytes(e) == bytes(r) {
                                    if r.ing < e.ing {
                                        kills += (e.loc != NOLOC) as usize;
                                        kill_loc(e.loc);
                                        valid_winners -= (e.loc != NOLOC) as usize;
                                        valid_winners += (r.loc != NOLOC) as usize;
                                        *e = *r;
                                    } else {
                                        kills += (r.loc != NOLOC) as usize;
                                        kill_loc(r.loc);
                                    }
                                    break;
                                }
                                if e.h == 0 {
                                    *e = *r;
                                    winners += 1;
                                    valid_winners += (r.loc != NOLOC) as usize;
                                    break;
                                }
                                i = (i + 1) & mask;
                            }
                        }
                    }
                    for s in p1.iter() {
                        s.ddl[p].recycle_into(&mut st.ar);
                    }
                }
                st.kills += kills;
                st.winners += winners;
                st.valid_winners += valid_winners;
                // bucket p of every worker is consumed: reuse its blocks for this worker's pushes
                for s in p1.iter() {
                    s.dd[p].recycle_into(&mut st.ar);
                }
                st.t_dd += tcpu() - tt;
            } else {
                let (w, a) = ctasks[p - npd];
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
    // phase 2b: per user partition, join its valid copies against that partition's links
    // (a small table, cache resident), skip killed copies, scatter by hash(person)
    let per_pv = BLK_BYTES / std::mem::size_of::<PRec>();
    let reg2 = Region::new(ev_bytes / 40 * std::mem::size_of::<PRec>() + nt * ((np + 2) * BLK_BYTES + SLAB));
    let p2 = par(
        nt,
        npu,
        |_| new_p2(reg2),
        |st, q| {
            let tt = tcpu();
            let links = links_wait();
            let lp = &links.parts[q];
            let mut sz = 16usize;
            while sz < lp.len() * 2 {
                sz *= 2;
            }
            let mask = sz - 1;
            let mut lt: Vec<(K, K)> = vec![(0, 0); sz];
            for &(k, v) in lp {
                let mut i = h128(k) as usize & mask;
                while lt[i].0 != 0 {
                    i = (i + 1) & mask;
                }
                lt[i] = (k, v);
            }
            let person = |k: K| -> K {
                let mut i = h128(k) as usize & mask;
                loop {
                    let e = lt[i];
                    if e.0 == k {
                        return e.1;
                    }
                    if e.0 == 0 {
                        return k;
                    }
                    i = (i + 1) & mask;
                }
            };
            for (w, s) in p1.iter().enumerate() {
                let o = off[w * npu + q];
                for (bi, sl) in s.pv[q].slices().enumerate() {
                    let base = o + bi * per_pv;
                    for (j, r) in sl.iter().enumerate() {
                        let g = base + j;
                        if (kill[g >> 6].load(Ordering::Relaxed) >> (g & 63)) & 1 != 0 {
                            continue;
                        }
                        let pk = person(r.pk);
                        st.ev[((h128(pk) >> 40) & pmask) as usize].push(&mut st.ar, PRec { pk, ts: r.ts, price: r.price, cat: r.cat });
                    }
                }
                s.pv[q].recycle_into(&mut st.ar);
            }
            st.t_map += tcpu() - tt;
        },
    );
    {
        let n: usize = p2.iter().map(|s| s.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
        let a: f64 = p2c.iter().map(|s| s.t_dd).sum();
        let b: f64 = p2.iter().map(|s| s.t_map).sum();
        let c: f64 = p2c.iter().map(|s| s.t_cl).sum();
        let kills: usize = p2c.iter().map(|s| s.kills).sum();
        let winners: usize = p2c.iter().map(|s| s.winners).sum();
        let vw: usize = p2c.iter().map(|s| s.valid_winners).sum();
        let copies: usize = p1.iter().map(|s| (s.dd.iter().map(|v| v.len()).sum::<usize>() + s.ddl.iter().map(|v| v.len()).sum::<usize>())).sum();
        // invariants: every valid copy is either the kept winner or killed; every copy is a winner or a loser
        let ok = nvalid == vw + kills && n == vw && copies >= winners;
        log(&format!(
            "dedup: {} surviving events (thread-s: dedup {:.2}, event person map {:.2}, click person map {:.2}); copies {} winners {} valid winners {} kills {} valid copies {} invariant {}",
            n, a, b, c, copies, winners, vw, kills, nvalid, if ok { "OK" } else { "FAIL" }
        ));
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
            for s in p2c.iter() {
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
            for s in p2c.iter() {
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

// ------------------------------------------------------------------ direct Parquet writer
/// Thrift compact protocol, just what a Parquet footer and page header need.
struct Tc {
    b: Vec<u8>,
    last: Vec<i16>, // last field id per open struct
}
impl Tc {
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let l = *self.last.last().unwrap();
        let d = id - l;
        if (1..=15).contains(&d) {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zz(v);
    }
    fn strf(&mut self, id: i16, s: &[u8]) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, ety: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
}

/// One column of a single-row-group, single-page (plus dictionary page), zstd Parquet file.
struct PCol<'a> {
    name: &'a str,
    ty: i32,          // 1 INT32, 2 INT64, 6 BYTE_ARRAY
    optional: bool,
    conv: Option<i32>, // converted type: 0 UTF8, 6 DATE
    logical: Option<i16>, // LogicalType union field: 1 STRING, 6 DATE
}

/// Writes a single-row-group Parquet file. For column c, `body(c, dict, page)` fills the data
/// page body (definition levels first for optional columns) and returns Some(n) when the column
/// is dictionary encoded: then `dict` holds the n PLAIN dictionary values and the page holds the
/// bit width byte + RLE/bit-packed indices (PLAIN_DICTIONARY). None means PLAIN values.
fn write_parquet_raw(path: &Path, cols: &[PCol], nrows: usize, mut body: impl FnMut(usize, &mut Vec<u8>, &mut Vec<u8>) -> Option<usize>) {
    // pages go straight to the file from reused buffers (no whole-file buffer)
    use std::io::Write;
    let mut file = std::fs::File::create(path).expect("create");
    file.write_all(b"PAR1").expect("write");
    let mut pos: usize = 4;
    // (data page offset, dictionary page offset, compressed size, uncompressed size, incl. headers)
    let mut metas: Vec<(i64, Option<i64>, i64, i64)> = Vec::new();
    let mut page: Vec<u8> = Vec::with_capacity(nrows * 8 + 64);
    let mut dict: Vec<u8> = Vec::new();
    // Pages are zstd compressed (ZL=0: uncompressed). The scorer's container flushes its dirty
    // output pages before exit (~2.7 ms per MB), so output bytes cost more than compression CPU.
    let lvl = zlevel();
    let mut zc = if lvl != 0 { Some(zstd::bulk::Compressor::new(lvl).expect("zstd")) } else { None };
    let mut zd: Vec<u8> = Vec::new();
    let mut zp: Vec<u8> = Vec::new();
    let mut squeeze = |src: &Vec<u8>, dst: &mut Vec<u8>| -> bool {
        if let Some(z) = zc.as_mut() {
            let t = Instant::now();
            dst.clear();
            dst.reserve(zstd::zstd_safe::compress_bound(src.len()));
            z.compress_to_buffer(src.as_slice(), dst).expect("zstd");
            ZNS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            true
        } else {
            false
        }
    };
    for c in 0..cols.len() {
        page.clear();
        dict.clear();
        let nd = body(c, &mut dict, &mut page);
        let mut size = 0usize;
        let mut usize_ = 0usize;
        let mut doff = None;
        if let Some(nd) = nd {
            let dz = if squeeze(&dict, &mut zd) { &zd } else { &dict };
            let mut h = Tc { b: Vec::new(), last: vec![0] };
            h.i32f(1, 2); // DICTIONARY_PAGE
            h.i32f(2, dict.len() as i32);
            h.i32f(3, dz.len() as i32);
            h.begin(7);
            h.i32f(1, nd as i32);
            h.i32f(2, 2); // PLAIN_DICTIONARY
            h.end();
            h.b.push(0);
            doff = Some(pos as i64);
            file.write_all(&h.b).expect("write");
            file.write_all(dz).expect("write");
            pos += h.b.len() + dz.len();
            size += h.b.len() + dz.len();
            usize_ += h.b.len() + dict.len();
        }
        let pz = if squeeze(&page, &mut zp) { &zp } else { &page };
        let mut h = Tc { b: Vec::new(), last: vec![0] };
        h.i32f(1, 0); // DATA_PAGE
        h.i32f(2, page.len() as i32);
        h.i32f(3, pz.len() as i32);
        h.begin(5);
        h.i32f(1, nrows as i32);
        h.i32f(2, if nd.is_some() { 2 } else { 0 }); // PLAIN_DICTIONARY / PLAIN
        h.i32f(3, 3); // RLE
        h.i32f(4, 3);
        h.end();
        h.b.push(0);
        let off = pos as i64;
        file.write_all(&h.b).expect("write");
        file.write_all(pz).expect("write");
        pos += h.b.len() + pz.len();
        size += h.b.len() + pz.len();
        usize_ += h.b.len() + page.len();
        metas.push((off, doff, size as i64, usize_ as i64));
    }
    let mut m = Tc { b: Vec::new(), last: vec![0] };
    m.i32f(1, 1);
    m.list(2, 12, cols.len() + 1);
    m.begin_elem();
    m.strf(4, b"schema");
    m.i32f(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.begin_elem();
        m.i32f(1, c.ty);
        m.i32f(3, c.optional as i32);
        m.strf(4, c.name.as_bytes());
        if let Some(v) = c.conv {
            m.i32f(6, v);
        }
        if let Some(l) = c.logical {
            m.begin(10);
            m.begin(l);
            m.end();
            m.end();
        }
        m.end();
    }
    m.i64f(3, nrows as i64);
    m.list(4, 12, 1);
    m.begin_elem();
    m.list(1, 12, cols.len());
    let total: i64 = metas.iter().map(|x| x.3).sum(); // uncompressed
    let codec = if lvl != 0 { 6 } else { 0 }; // ZSTD / UNCOMPRESSED
    for (c, &(off, doff, sz, usz)) in cols.iter().zip(metas.iter()) {
        m.begin_elem();
        m.i64f(2, doff.unwrap_or(off));
        m.begin(3);
        m.i32f(1, c.ty);
        if doff.is_some() {
            m.list(2, 5, 3);
            m.zz(0); // PLAIN
            m.zz(3); // RLE
            m.zz(2); // PLAIN_DICTIONARY
        } else {
            m.list(2, 5, 2);
            m.zz(0);
            m.zz(3);
        }
        m.list(3, 8, 1);
        m.varint(c.name.len() as u64);
        m.b.extend_from_slice(c.name.as_bytes());
        m.i32f(4, codec);
        m.i64f(5, nrows as i64);
        m.i64f(6, usz);
        m.i64f(7, sz);
        m.i64f(9, off);
        if let Some(d) = doff {
            m.i64f(11, d);
        }
        m.end();
        m.end();
    }
    m.i64f(2, total);
    m.i64f(3, nrows as i64);
    m.end();
    m.strf(6, b"sessions");
    m.b.push(0);
    let n = m.b.len() as u32;
    m.b.extend_from_slice(&n.to_le_bytes());
    m.b.extend_from_slice(b"PAR1");
    file.write_all(&m.b).expect("write");
    let nw = 2 + metas.iter().map(|x| if x.1.is_some() { 4 } else { 2 }).sum::<usize>();
    drop(file);
    tlog(&format!("wrote {} ({} bytes, {} write calls, closed)", path.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default(), pos + m.b.len(), nw));
}

/// total zstd compression time over all writer threads (ns), logged with the done line
static ZNS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// zstd level for output pages: ZL env (0 = uncompressed, negative = zstd fast levels), default 1.
fn zlevel() -> i32 {
    std::env::var("ZL").ok().and_then(|v| v.parse().ok()).unwrap_or(1)
}

fn uleb(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Bits needed for values 0..=max (at least 1).
fn bit_width(max: u64) -> u32 {
    (64 - max.leading_zeros()).max(1)
}

/// One bit-packed run of the RLE/bit-packed hybrid: all n values, groups of 8 (zero padded).
fn hybrid_bitpack(vals: impl Iterator<Item = u32>, n: usize, bw: u32, out: &mut Vec<u8>) {
    let groups = (n + 7) / 8;
    uleb(((groups as u64) << 1) | 1, out);
    let start = out.len();
    out.resize(start + groups * bw as usize, 0);
    let buf = &mut out[start..];
    let mut acc: u64 = 0;
    let mut nb: u32 = 0;
    let mut o = 0usize;
    for v in vals {
        acc |= (v as u64) << nb;
        nb += bw;
        while nb >= 8 {
            buf[o] = acc as u8;
            o += 1;
            acc >>= 8;
            nb -= 8;
        }
    }
    if nb > 0 {
        buf[o] = acc as u8;
    }
}

/// RLE runs of the hybrid encoding (for indices that repeat in runs).
fn hybrid_rle(vals: &[u32], bw: u32, out: &mut Vec<u8>) {
    let nbytes = ((bw + 7) / 8) as usize;
    let mut i = 0;
    while i < vals.len() {
        let v = vals[i];
        let mut j = i + 1;
        while j < vals.len() && vals[j] == v {
            j += 1;
        }
        uleb(((j - i) as u64) << 1, out);
        out.extend_from_slice(&v.to_le_bytes()[..nbytes]);
        i = j;
    }
}

/// Integer column: dictionary (<= 65536 distinct values) with bit-packed indices, else PLAIN.
/// `w32`: the column's physical type is INT32 (every value of the table fits).
fn int_column(v: &[i64], w32: bool, dict: &mut Vec<u8>, page: &mut Vec<u8>) -> Option<usize> {
    let mut map: hashbrown::HashMap<i64, u32, foldhash::fast::FixedState> = Default::default();
    let mut idx: Vec<u32> = Vec::with_capacity(v.len());
    let mut vals: Vec<i64> = Vec::new();
    for &x in v {
        let n = vals.len() as u32;
        let i = *map.entry(x).or_insert_with(|| {
            vals.push(x);
            n
        });
        idx.push(i);
        if vals.len() > 65536 {
            break;
        }
    }
    let put = |x: i64, o: &mut Vec<u8>| {
        if w32 {
            o.extend_from_slice(&(x as i32).to_le_bytes());
        } else {
            o.extend_from_slice(&x.to_le_bytes());
        }
    };
    if vals.len() > 65536 {
        for &x in v {
            put(x, page);
        }
        return None;
    }
    for &x in &vals {
        put(x, dict);
    }
    let bw = bit_width(vals.len() as u64 - 1);
    page.push(bw as u8);
    hybrid_bitpack(idx.iter().copied(), idx.len(), bw, page);
    Some(vals.len())
}

/// `w32[k]`: sessions / events / revenue_cents are written as INT32 (all values of the table fit).
fn write_user_raw(path: &Path, r: &URows, cats: &[S], w32: [bool; 3]) {
    let it = |w: bool| if w { 1 } else { 2 };
    let cols = [
        PCol { name: "user_id", ty: 6, optional: false, conv: Some(0), logical: Some(1) },
        PCol { name: "day", ty: 1, optional: false, conv: Some(6), logical: Some(6) },
        PCol { name: "sessions", ty: it(w32[0]), optional: false, conv: None, logical: None },
        PCol { name: "events", ty: it(w32[1]), optional: false, conv: None, logical: None },
        PCol { name: "revenue_cents", ty: it(w32[2]), optional: false, conv: None, logical: None },
        PCol { name: "top_category", ty: 6, optional: true, conv: Some(0), logical: Some(1) },
    ];
    let n = r.name.len();
    let mut idb: Vec<u8> = Vec::new();
    write_parquet_raw(path, &cols, n, |c, dict, out| match c {
        0 => {
            // a person's rows are contiguous: dictionary = names in row order, deduplicated
            // between neighbours; indices are runs (RLE)
            let mut idx: Vec<u32> = Vec::with_capacity(n);
            let mut nd = 0u32;
            for (i, &k) in r.name.iter().enumerate() {
                if i == 0 || r.name[i - 1] != k {
                    idb.clear();
                    id_bytes(k, &mut idb);
                    dict.extend_from_slice(&(idb.len() as u32).to_le_bytes());
                    dict.extend_from_slice(&idb);
                    nd += 1;
                }
                idx.push(nd - 1);
            }
            let bw = bit_width(nd as u64 - 1);
            out.push(bw as u8);
            hybrid_rle(&idx, bw, out);
            Some(nd as usize)
        }
        1 => {
            let lo = *r.day.iter().min().unwrap();
            let hi = *r.day.iter().max().unwrap();
            for d in lo..=hi {
                dict.extend_from_slice(&d.to_le_bytes());
            }
            let bw = bit_width((hi - lo) as u64);
            out.push(bw as u8);
            hybrid_bitpack(r.day.iter().map(|&d| (d - lo) as u32), n, bw, out);
            Some((hi - lo + 1) as usize)
        }
        2 | 3 | 4 => int_column([&r.ses, &r.ev, &r.rev][c - 2], w32[c - 2], dict, out),
        _ => {
            def_levels(r.top.iter().map(|&t| t != NOCAT), n, out);
            for s in cats {
                dict.extend_from_slice(&(s.len() as u32).to_le_bytes());
                dict.extend_from_slice(s);
            }
            let nn = r.top.iter().filter(|&&t| t != NOCAT).count();
            let bw = bit_width(cats.len() as u64 - 1);
            out.push(bw as u8);
            hybrid_bitpack(r.top.iter().filter(|&&t| t != NOCAT).copied(), nn, bw, out);
            Some(cats.len())
        }
    });
}

/// campaign_daily through the direct writer: rows are ordered by campaign then day, so
/// campaign_id and channel are dictionaries with RLE indices; day and the counters as in user_daily.
fn write_campaign_raw(path: &Path, rows: &[(S, S, i32, [i64; 4])]) {
    let n = rows.len();
    // counters stay INT64 as in the crate writer (the table is small; zstd takes the slack)
    let fit = [false; 4];
    let it = |w: bool| if w { 1 } else { 2 };
    let cols = [
        PCol { name: "campaign_id", ty: 6, optional: false, conv: Some(0), logical: Some(1) },
        PCol { name: "channel", ty: 6, optional: false, conv: Some(0), logical: Some(1) },
        PCol { name: "day", ty: 1, optional: false, conv: Some(6), logical: Some(6) },
        PCol { name: "sessions", ty: it(fit[0]), optional: false, conv: None, logical: None },
        PCol { name: "purchasing_sessions", ty: it(fit[1]), optional: false, conv: None, logical: None },
        PCol { name: "revenue_cents", ty: it(fit[2]), optional: false, conv: None, logical: None },
        PCol { name: "assisted_sessions", ty: it(fit[3]), optional: false, conv: None, logical: None },
    ];
    let mut v: Vec<i64> = Vec::with_capacity(n);
    write_parquet_raw(path, &cols, n, |c, dict, out| match c {
        0 | 1 => {
            // dictionary of distinct strings (first-seen order), RLE indices
            let mut map: hashbrown::HashMap<S, u32, foldhash::fast::FixedState> = Default::default();
            let mut idx: Vec<u32> = Vec::with_capacity(n);
            for r in rows {
                let s = if c == 0 { r.0 } else { r.1 };
                let nd = map.len() as u32;
                let i = *map.entry(s).or_insert_with(|| {
                    dict.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    dict.extend_from_slice(s);
                    nd
                });
                idx.push(i);
            }
            let bw = bit_width(map.len() as u64 - 1);
            out.push(bw as u8);
            hybrid_rle(&idx, bw, out);
            Some(map.len())
        }
        2 => {
            let lo = rows.iter().map(|r| r.2).min().unwrap();
            let hi = rows.iter().map(|r| r.2).max().unwrap();
            for d in lo..=hi {
                dict.extend_from_slice(&d.to_le_bytes());
            }
            let bw = bit_width((hi - lo) as u64);
            out.push(bw as u8);
            hybrid_bitpack(rows.iter().map(|r| (r.2 - lo) as u32), n, bw, out);
            Some((hi - lo + 1) as usize)
        }
        _ => {
            v.clear();
            v.extend(rows.iter().map(|r| r.3[c - 3]));
            int_column(&v, fit[c - 3], dict, out)
        }
    });
}

/// Definition levels (max level 1) as one RLE/bit-packed hybrid run set, with the 4-byte length.
fn def_levels(defs: impl Iterator<Item = bool>, n: usize, out: &mut Vec<u8>) {
    // bit-packed groups of 8: header = (groups << 1) | 1, then ceil(n / 8) bytes
    let groups = (n + 7) / 8;
    let mut hdr = Vec::new();
    let mut v = ((groups as u64) << 1) | 1;
    while v >= 0x80 {
        hdr.push((v as u8) | 0x80);
        v >>= 7;
    }
    hdr.push(v as u8);
    let len = hdr.len() + groups;
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.extend_from_slice(&hdr);
    let start = out.len();
    out.resize(start + groups, 0);
    for (i, d) in defs.enumerate() {
        if d {
            out[start + i / 8] |= 1 << (i % 8);
        }
    }
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
    /// Differential test: phase-3 time-bucket sessions == sort + gap sweep, on random persons with gaps of
    /// exactly GAP - 1 / GAP / GAP + 1, equal timestamps, purchases on session edges, any input order,
    /// ts at both ends of the u32 range, and one reused bucket state (it must be empty after every call).
    #[test]
    fn sessions_bucket_matches_sort() {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut rnd = |m: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % m
        };
        let g = GAP as u64;
        let mut bk = Bk::new();
        let (mut s1, mut p1, mut s2, mut p2) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut nses = 0usize;
        for it in 0..20_000u64 {
            let n = if it % 500 == 0 { 3000 } else { rnd(60) as usize };
            let mut t: u64 = match rnd(4) {
                0 => rnd(4 * g),
                1 => (1u64 << 32) - 1 - rnd(n as u64 * 3 * g + 1).min((1u64 << 32) - 1),
                2 => rnd(1 << 31) / g * g, // on a bucket edge
                _ => rnd(1 << 32),
            };
            let mut e: Vec<u64> = Vec::with_capacity(n);
            for _ in 0..n {
                if t >= 1u64 << 32 {
                    break;
                }
                let pc = if rnd(5) == 0 { ((1 + rnd(1 << 20)) << 8 | rnd(256)) as u32 } else { 0 };
                e.push(t << 32 | pc as u64);
                t += match rnd(9) {
                    0 => 0,
                    1 => g - 1,
                    2 => g,
                    3 => g + 1,
                    4 => 1,
                    5 => rnd(g),
                    6 => rnd(3 * g),
                    7 => 86_400_000 - rnd(2),
                    _ => rnd(WEEK as u64),
                };
            }
            // shuffle: the bucket path must not depend on input order
            for i in (1..e.len()).rev() {
                let j = rnd(i as u64 + 1) as usize;
                e.swap(i, j);
            }
            sessions_bucket(&e, &mut bk, &mut s1, &mut p1);
            let mut es = e.clone();
            sessions_sort(&mut es, &mut s2, &mut p2);
            assert_eq!(s1, s2, "sessions differ (iteration {})", it);
            assert_eq!(p1, p2, "purchases differ (iteration {})", it);
            nses += s1.len();
        }
        assert!(bk.bits.iter().all(|&w| w == 0) && bk.bcnt.iter().all(|&c| c == 0));
        assert!(bk.bmin.iter().all(|&m| m == u32::MAX) && bk.bmax.iter().all(|&m| m == 0));
        assert!(nses > 100_000);
    }
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
            let mut c: Vec<Vec<Vec<u8>>> = Vec::new();
            let mut w: Vec<Vec<Vec<u8>>> = Vec::new();
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
                for_rows128::<N>(data, s, e, |f| c.push(f.iter().map(|x| x.to_vec()).collect()));
                for_rows_win::<N>(data, s, e, |f| w.push(f.iter().map(|x| x.to_vec()).collect()));
                s = e;
            }
            assert_eq!(a.len(), b.len());
            assert!(a == b, "mismatch seed {} ch {}", seed, ch);
            assert!(a == c, "mismatch (128) seed {} ch {}", seed, ch);
            assert!(a == w, "mismatch (win) seed {} ch {}", seed, ch);
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
    /// parse_ts_win == parse_ts on every value inside its window; ingest_ms_win == ingest_ms.
    #[test]
    fn parse_ts_win_matches() {
        // windows ending/starting at month and leap-day boundaries
        let wins: [(&str, i64); 5] = [("2028-02-20T00:00:00Z", 14), ("2027-12-25T00:00:00Z", 14), ("2026-10-01T00:00:00Z", 21), ("2025-01-28T00:00:00Z", 30), ("2028-02-29T00:00:00Z", 1)];
        let alpha = b"0123456789-T:Z+";
        let mut x = 7u64;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for &(ws, nd) in &wins {
            let lo = parse_ts(ws.as_bytes()).unwrap();
            let hi = lo + nd * DAY;
            let w = Win::new(lo, hi).unwrap();
            let mut inside = 0;
            for k in 0..300000 {
                // a random instant within +-3 days of the window, printed in F1/F2/F3, then mutated
                let t = lo - 3 * DAY + (rnd() % ((nd + 6) * DAY) as u64) as i64 / 1000 * 1000;
                let off = (rnd() % (24 * 60)) as i64 - 12 * 60;
                let form = rnd() % 3;
                let loc = t + off * 60_000;
                let (y, m, d) = civil_from_days(loc.div_euclid(DAY));
                let sod = loc.rem_euclid(DAY) / 1000;
                let mut v: Vec<u8> = match form {
                    0 => format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, sod / 3600, sod / 60 % 60, sod % 60).into_bytes(),
                    1 => format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}{:02}:{:02}", y, m, d, sod / 3600, sod / 60 % 60, sod % 60, if off >= 0 { '+' } else { '-' }, off.abs() / 60, off.abs() % 60).into_bytes(),
                    _ => format!("{}", t).into_bytes(),
                };
                if form == 2 {
                    v = format!("{}", t).into_bytes();
                }
                if k % 3 == 0 && !v.is_empty() {
                    let p = (rnd() as usize) % v.len();
                    v[p] = alpha[(rnd() as usize) % alpha.len()];
                }
                let r = parse_ts(&v);
                let g = parse_ts_win(&v, &w);
                if let Some(tv) = r {
                    if tv >= lo && tv < hi {
                        inside += 1;
                        assert_eq!(g, r, "{:?}", String::from_utf8_lossy(&v));
                    }
                }
                if let Some(gv) = g {
                    assert_eq!(Some(gv), r, "{:?}", String::from_utf8_lossy(&v));
                }
                if form == 0 && r.is_some() {
                    assert_eq!(ingest_ms_win(&v, &w), ingest_ms(&v));
                }
            }
            assert!(inside > 20000, "{}", inside);
        }
    }
    #[test]
    fn pack16_matches_pack() {
        let alpha: &[u8] = b"-09AZ_az\t\xc3 .~,\"/:@[`{\x7f\x80\xff";
        let mut x = 4242u64;
        let mut buf: Vec<u8> = Vec::new();
        let mut spans = Vec::new();
        for _ in 0..200000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let n = (x % 13) as usize;
            let s = buf.len();
            for j in 0..n {
                let y = x.rotate_left(5 * j as u32);
                buf.push(if y % 3 == 0 { alpha[(y >> 8) as usize % alpha.len()] } else { SYM[(y >> 16) as usize % 64] });
            }
            spans.push((s, n));
            buf.push(b',');
        }
        let data: S = Box::leak(buf.into_boxed_slice());
        let lim = data.as_ptr() as usize + data.len();
        for &(s, n) in &spans {
            let f = &data[s..s + n];
            assert_eq!(pack16(f, lim), pack(f), "{:?}", f);
        }
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
    fn days_fast_matches() {
        for y in [-5i64, 0, 1, 1899, 1900, 1970, 2000, 2024, 2028, 2100, 2299, 2300, 9999] {
            for m in 0..=13 {
                for d in 0..=32 {
                    assert_eq!(days_fast(y, m, d), days_from_civil(y, m, d), "{} {} {}", y, m, d);
                }
            }
        }
    }
    /// z23 fast key: the 8-byte-load path == the slice path; fast keys exist exactly for
    /// 2..=9 ASCII bytes without NUL; equality == byte equality, order == byte order, and
    /// id_bytes round-trips, over random and adversarial IDs (uppercase, 10+ bytes, non-alnum,
    /// NUL, high bytes, empty).
    #[test]
    fn kfast_matches_slice_and_bytes() {
        let alpha: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCZ-_ ,.\"\x00\x7f\x80\xc3\xff\t";
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut ids: Vec<Vec<u8>> = vec![Vec::new(), b"e".to_vec(), b"\x00".to_vec(), b"u\x00".to_vec(), b"\x80a".to_vec(), b"uABCDEFGH".to_vec(), b"u12345678".to_vec(), b"u123456789".to_vec(), b"e0".to_vec(), b"e00".to_vec(), b"e0\x00".to_vec()];
        for i in 0..40000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let n = (x % 14) as usize;
            // half: generator-shaped (prefix + base36), half: any alphabet byte
            let v: Vec<u8> = (0..n).map(|j| { let y = x.rotate_left(7 * j as u32) >> 3; if i % 2 == 0 { if j == 0 { b"euk"[(y % 3) as usize] } else { alpha[(y % 36) as usize] } } else { alpha[(y % alpha.len() as u64) as usize] } }).collect();
            ids.push(v);
        }
        let mut buf: Vec<u8> = Vec::new();
        let mut spans = Vec::new();
        for v in &ids {
            spans.push((buf.len(), v.len()));
            buf.extend_from_slice(v);
        }
        let data: S = Box::leak(buf.into_boxed_slice());
        let lim = data.as_ptr() as usize + data.len();
        let mut nfast = 0;
        for (i, &(a, n)) in spans.iter().enumerate() {
            let f = &data[a..a + n];
            let want = n >= 2 && n <= 9 && f.iter().all(|&c| c != 0 && c < 0x80);
            assert_eq!(kfast(f, lim), kfast_s(f), "{:?}", f);
            assert_eq!(kfast_s(f).is_some(), want, "{:?}", f);
            nfast += want as usize;
            assert_eq!(f, &ids[i][..]);
        }
        assert!(nfast > 15000);
        let ks: Vec<K> = ids.iter().map(|v| idk(Box::leak(v.clone().into_boxed_slice()))).collect();
        let kl: Vec<K> = spans.iter().map(|&(a, n)| idkf(&data[a..a + n], lim)).collect();
        freeze_long();
        for i in 0..ids.len() {
            let mut b = Vec::new();
            id_bytes(ks[i], &mut b);
            assert_eq!(b, ids[i]);
            assert_eq!(ks[i], kl[i]);
            for j in [(i * 7919) % ids.len(), (i + 1) % ids.len(), (i * 31 + 5) % ids.len()] {
                assert_eq!(id_cmp(ks[i], ks[j]), ids[i].cmp(&ids[j]), "{:?} {:?}", ids[i], ids[j]);
                assert_eq!(ks[i] == ks[j], ids[i] == ids[j]);
            }
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
    fn dim_table_matches() {
        for i in 0..MYN * 12 {
            let y = MY0 + (i / 12) as i64;
            let m = (i % 12) as i64 + 1;
            let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
            let dim = match m {
                2 => 28 + leap as i64,
                4 | 6 | 9 | 11 => 30,
                _ => 31,
            };
            assert_eq!(DIM0[i] as i64, dim, "{} {}", y, m);
        }
    }
    #[test]
    fn ingest_fuzz_matches_scalar() {
        // the old scalar ingest formula on any 20-byte 'Z' value
        let old = |s: &[u8]| {
            let d = |i: usize| (s[i].wrapping_sub(b'0')) as i64;
            let y = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
            let mo = d(5) * 10 + d(6);
            let dd = d(8) * 10 + d(9);
            let h = d(11) * 10 + d(12);
            let mi = d(14) * 10 + d(15);
            let se = d(17) * 10 + d(18);
            ((days_fast(y, mo, dd) * 24 + h) * 60 + mi) * 60_000 + se * 1000
        };
        let mut x = 7u64;
        for _ in 0..400000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let mut v = b"2027-12-09T00:00:00Z".to_vec();
            for (i, b) in v.iter_mut().enumerate().take(19) {
                if b.is_ascii_digit() {
                    *b = b'0' + ((x >> (i * 3 % 61)) % 10) as u8;
                }
            }
            if (x >> 60) == 0 {
                let i = (x % 19) as usize;
                v[i] = b"-T:Z/ a"[((x >> 20) % 7) as usize];
            }
            assert_eq!(ingest_ms(&v), old(&v), "{:?}", String::from_utf8_lossy(&v));
        }
    }
    #[test]
    fn ingest_matches_parse_ts() {
        for s in ["2027-12-09T00:00:00Z", "2028-02-29T23:59:59Z", "1999-12-31T12:34:56Z", "2026-10-15T09:08:07Z"] {
            assert_eq!(ingest_ms(s.as_bytes()), parse_ts(s.as_bytes()).unwrap());
        }
    }
}

#[cfg(test)]
mod bench_ts {
    #![allow(dead_code)]
    use super::*;
    pub fn old_ts_fast(s: &[u8]) -> Option<i64> {
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
                let base = ((days_fast(y, mo, dd) * 24 + h) * 60 + mi) * 60_000 + se * 1000;
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
            return super::parse_ts(s);
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
        super::parse_ts(s)
    }
    pub fn old_ingest(s: &[u8]) -> i64 {
        if s.len() == 20 && s[19] == b'Z' {
            let d = |i: usize| (s[i].wrapping_sub(b'0')) as i64;
            let y = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
            let mo = d(5) * 10 + d(6);
            let dd = d(8) * 10 + d(9);
            let h = d(11) * 10 + d(12);
            let mi = d(14) * 10 + d(15);
            let se = d(17) * 10 + d(18);
            ((days_fast(y, mo, dd) * 24 + h) * 60 + mi) * 60_000 + se * 1000
        } else {
            parse_ts(s).unwrap_or(i64::MAX / 4)
        }
    }
    /// TSBENCH=<events csv>: ns per ts / ingest, old (a25) vs current, on real strings in file order.
    #[test]
    #[ignore]
    fn ts_bench() {
        let path = std::env::var("TSBENCH").unwrap();
        let mut v = std::fs::read(path).unwrap();
        let cap: usize = std::env::var("TSCAP").ok().and_then(|x| x.parse().ok()).unwrap_or(4 << 20);
        if v.len() > cap {
            let e = cap + v[cap..].iter().position(|&c| c == 10u8).unwrap() + 1;
            v.truncate(e);
        }
        let data: S = Box::leak(v.into_boxed_slice());
        let mut ts: Vec<S> = Vec::new();
        let mut ing: Vec<S> = Vec::new();
        let lim = data.as_ptr() as usize + data.len();
        for_rows::<7>(data, 0, data.len(), |f| {
            ts.push(f[2]);
            ing.push(f[3]);
        });
        for (i, t) in ts.iter().enumerate() {
            assert_eq!(parse_ts_fast(t), old_ts_fast(t));
            assert_eq!(ingest_ms(ing[i]), old_ingest(ing[i]));
        }
        let vs: Vec<(&str, Box<dyn Fn() -> i64>)> = vec![
            ("ts old", Box::new(|| ts.iter().map(|t| std::hint::black_box(old_ts_fast(std::hint::black_box(t))).unwrap_or(0)).fold(0, |a, b| a ^ b))),
            ("ts new", Box::new(|| ts.iter().map(|t| std::hint::black_box(parse_ts_fast(std::hint::black_box(t))).unwrap_or(0)).fold(0, |a, b| a ^ b))),
            ("ingest old", Box::new(|| ing.iter().map(|t| std::hint::black_box(old_ingest(std::hint::black_box(t)))).fold(0, |a, b| a ^ b))),
            ("ingest new", Box::new(|| ing.iter().map(|t| std::hint::black_box(ingest_ms(std::hint::black_box(t)))).fold(0, |a, b| a ^ b))),
            ("tok new", Box::new(|| {
                let mut n = 0i64;
                for_rows::<7>(data, 0, data.len(), |f| n += f[2].len() as i64 ^ f[6].len() as i64);
                n
            })),
            ("tok128", Box::new(|| {
                let mut n = 0i64;
                for_rows128::<7>(data, 0, data.len(), |f| n += f[2].len() as i64 ^ f[6].len() as i64);
                n
            })),
        ];
        let mut best = vec![f64::MAX; vs.len()];
        let mut acc = 0;
        for _ in 0..40 {
            for (i, v) in vs.iter().enumerate() {
                let t0 = Instant::now();
                acc ^= (v.1)();
                best[i] = best[i].min(t0.elapsed().as_secs_f64());
            }
        }
        let line: Vec<String> = vs.iter().zip(&best).map(|(v, b)| format!("{} {:.2}", v.0, b * 1e9 / ts.len() as f64)).collect();
        eprintln!("ns/item (min of 40, {} items): {} [{}]", ts.len(), line.join(" | "), acc & 1);
    }
}
