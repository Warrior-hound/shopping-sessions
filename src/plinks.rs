

















use super::*;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Mutex, OnceLock};

const CHUNK: usize = 512 << 10;

struct ChunkOut {
    pairs: Vec<(K, K)>,
    pos: Vec<(u32, u32)>,


    keys: Vec<AtomicU64>,
    offs: Vec<usize>,
}

struct PartTab {
    ids: Vec<K>,
}

thread_local! {

    static BTAB: std::cell::RefCell<Vec<u32>> = const { std::cell::RefCell::new(Vec::new()) };
}


fn zeroed_u64(n: usize) -> Vec<AtomicU64> {
    let v = std::mem::ManuallyDrop::new(vec![0u64; n]);

    unsafe { Vec::from_raw_parts(v.as_ptr() as *mut AtomicU64, v.len(), v.capacity()) }
}

pub struct LinkJob {
    np: usize,
    chunks: Vec<(S, usize, usize)>,
    nsub: [usize; 5],
    stage: AtomicUsize,
    claimed: [AtomicUsize; 5],
    finished: [AtomicUsize; 5],
    tns: [AtomicU64; 5],
    a: Vec<OnceLock<ChunkOut>>,
    b: Vec<OnceLock<PartTab>>,
    base: OnceLock<Vec<u32>>,
    parent: OnceLock<&'static [AtomicU32]>,
    best: OnceLock<&'static [AtomicU64]>,
    e: Vec<Mutex<Vec<(K, K)>>>,
    out: OnceLock<Links>,
    t0: Instant,
}

#[inline(always)]
fn part(k: K, np: usize) -> usize {
    ((ph(k) >> 40) as usize) & (np - 1)
}

#[inline(always)]
fn find(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = p[x as usize].load(Ordering::Relaxed);
        if px == 0 {
            return x;
        }
        let y = px - 1;
        let py = p[y as usize].load(Ordering::Relaxed);
        if py == 0 {
            return y;
        }


        p[x as usize].store(py, Ordering::Relaxed);
        x = py - 1;
    }
}

#[inline(always)]
fn union(p: &[AtomicU32], a: u32, b: u32) {
    let (mut a, mut b) = (a, b);
    loop {
        a = find(p, a);
        b = find(p, b);
        if a == b {
            return;
        }
        let (hi, lo) = if a > b { (a, b) } else { (b, a) };
        if p[hi as usize].compare_exchange(0, lo + 1, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            return;
        }
    }
}

impl LinkJob {

    pub fn new(files: &[S], np: usize) -> LinkJob {
        Self::with_chunk(files, np, CHUNK)
    }

    pub fn with_chunk(files: &[S], np: usize, chunk: usize) -> LinkJob {
        assert!(np.is_power_of_two() && chunk > 0);
        let mut chunks = Vec::new();
        for &d in files {
            let mut s = 0;
            while s < d.len() {
                let e = (s + chunk).min(d.len());
                chunks.push((d, s, e));
                s = e;
            }
        }
        let nc = chunks.len();
        LinkJob {
            np,
            nsub: [nc, np, nc, np, np],
            stage: AtomicUsize::new(0),
            claimed: Default::default(),
            finished: Default::default(),
            tns: Default::default(),
            a: (0..nc).map(|_| OnceLock::new()).collect(),
            b: (0..np).map(|_| OnceLock::new()).collect(),
            base: OnceLock::new(),
            parent: OnceLock::new(),
            best: OnceLock::new(),
            e: (0..np).map(|_| Mutex::new(Vec::new())).collect(),
            out: OnceLock::new(),
            chunks,
            t0: Instant::now(),
        }
    }

    pub fn done(&self) -> Option<&Links> {
        self.out.get()
    }



    pub fn help(&self) -> bool {
        loop {
            let s = self.stage.load(Ordering::Acquire);
            if s >= 5 {
                return false;
            }
            if self.nsub[s] == 0 {


                if self.claimed[s].fetch_add(1, Ordering::Relaxed) == 0 {
                    self.finish_stage(s);
                    continue;
                }
                return false;
            }
            let i = self.claimed[s].fetch_add(1, Ordering::Relaxed);
            if i >= self.nsub[s] {
                return false;
            }
            let c0 = tcpu();
            match s {
                0 => self.stage_a(i),
                1 => self.stage_b(i),
                2 => self.stage_c(i),
                3 => self.stage_d(i),
                _ => self.stage_e(i),
            }
            self.tns[s].fetch_add(((tcpu() - c0) * 1e9) as u64, Ordering::Relaxed);
            if self.finished[s].fetch_add(1, Ordering::AcqRel) + 1 == self.nsub[s] {
                self.finish_stage(s);
            }
            return true;
        }
    }


    pub fn run_to_end(&self) -> &Links {
        loop {
            if let Some(l) = self.out.get() {
                return l;
            }
            if !self.help() {
                std::thread::yield_now();
            }
        }
    }

    fn finish_stage(&self, s: usize) {
        let c0 = tcpu();
        match s {
            1 => {
                let mut base = Vec::with_capacity(self.np + 1);
                let mut n = 0u64;
                for q in 0..self.np {
                    base.push(n as u32);
                    n += self.b[q].get().unwrap().ids.len() as u64;
                }
                assert!(n < u32::MAX as u64);
                base.push(n as u32);
                let n = n as usize;
                let _ = self.base.set(base);


                let pr = huge_region(n * 4) as *const AtomicU32;
                let br = huge_region(n * 8) as *const AtomicU64;
                let _ = self.parent.set(unsafe { std::slice::from_raw_parts(pr, n) });
                let _ = self.best.set(unsafe { std::slice::from_raw_parts(br, n) });
            }
            4 => {
                let parts: Vec<Vec<(K, K)>> = self.e.iter().map(|m| std::mem::take(&mut *m.lock().unwrap())).collect();
                let n = self.base.get().map(|b| b[self.np] as usize).unwrap_or(0);
                let pairs: usize = self.a.iter().map(|c| c.get().map(|c| c.pairs.len()).unwrap_or(0)).sum();
                let ts = |i: usize| self.tns[i].load(Ordering::Relaxed) as f64 * 1e-9;
                log(&format!(
                    "links built (parallel): {} pairs, {} ids, {} chunks, thread-s parse {:.3} intern {:.3} union {:.3} best {:.3} emit {:.3} (total {:.3}), wall {:.3} s",
                    pairs,
                    n,
                    self.chunks.len(),
                    ts(0),
                    ts(1),
                    ts(2),
                    ts(3),
                    ts(4),
                    (0..5).map(ts).sum::<f64>(),
                    self.t0.elapsed().as_secs_f64()
                ));
                let _ = self.out.set(Links { tab: &[], mask: 0, n, parts });
            }
            _ => {}
        }
        if s < 5 {
            self.tns[s].fetch_add(((tcpu() - c0) * 1e9) as u64, Ordering::Relaxed);
        }
        self.stage.store(s + 1, Ordering::Release);
    }

    fn stage_a(&self, i: usize) {
        let (d, s, e) = self.chunks[i];
        let np = self.np;
        let mut pairs: Vec<(K, K)> = Vec::with_capacity((e - s) / 16);
        let mut f: [S; 2] = [b""; 2];
        for_lines(d, s, e, |l| {
            if split(l, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                pairs.push((idk(f[0]), idk(f[1])));
            }
        });
        let mut offs = vec![0usize; np + 1];
        for &(a, b) in &pairs {
            offs[part(a, np) + 1] += 1;
            offs[part(b, np) + 1] += 1;
        }
        for q in 0..np {
            offs[q + 1] += offs[q];
        }
        let mut cur = offs.clone();
        let keys = zeroed_u64(2 * pairs.len());
        let mut pos: Vec<(u32, u32)> = Vec::with_capacity(pairs.len());
        for &(a, b) in &pairs {
            let (qa, qb) = (part(a, np), part(b, np));
            let pa = cur[qa];
            cur[qa] += 1;
            let pb = cur[qb];
            cur[qb] += 1;
            keys[pa].store(a, Ordering::Relaxed);
            keys[pb].store(b, Ordering::Relaxed);
            pos.push((pa as u32, pb as u32));
        }
        let _ = self.a[i].set(ChunkOut { pairs, pos, keys, offs });
    }

    fn stage_b(&self, q: usize) {
        let m: usize = self.a.iter().map(|c| { let c = c.get().unwrap(); c.offs[q + 1] - c.offs[q] }).sum();
        let mut sz = 16usize;
        while sz < m + m / 2 + 1 {
            sz *= 2;
        }
        let mask = sz - 1;
        let mut ids: Vec<K> = Vec::with_capacity(m / 2 + 1);
        BTAB.with(|bt| {
            let mut tab = bt.borrow_mut();
            tab.clear();
            tab.resize(sz, 0);
            for c in &self.a {
                let c = c.get().unwrap();
                let ks = &c.keys[c.offs[q]..c.offs[q + 1]];
                for (j, e) in ks.iter().enumerate() {
                    if j + PF < ks.len() {
                        prefetch(&tab[ht(ks[j + PF].load(Ordering::Relaxed)) as usize & mask]);
                    }
                    let k = e.load(Ordering::Relaxed);
                    let mut s = ht(k) as usize & mask;
                    let id = loop {
                        let t = unsafe { *tab.get_unchecked(s) };
                        if t == 0 {
                            let id = ids.len() as u32;
                            ids.push(k);
                            unsafe { *tab.get_unchecked_mut(s) = id + 1 };
                            break id;
                        }
                        if unsafe { *ids.get_unchecked(t as usize - 1) } == k {
                            break t - 1;
                        }
                        s = (s + 1) & mask;
                    };
                    e.store(id as K, Ordering::Relaxed);
                }
            }
        });
        let _ = self.b[q].set(PartTab { ids });
    }

    fn stage_c(&self, i: usize) {
        let np = self.np;
        let c = self.a[i].get().unwrap();
        let base = self.base.get().unwrap();
        let p = self.parent.get().unwrap();
        for (&(a, b), &(pa, pb)) in c.pairs.iter().zip(c.pos.iter()) {
            let ga = base[part(a, np)] + c.keys[pa as usize].load(Ordering::Relaxed) as u32;
            let gb = base[part(b, np)] + c.keys[pb as usize].load(Ordering::Relaxed) as u32;
            union(p, ga, gb);
        }
    }

    fn stage_d(&self, q: usize) {
        let t = self.b[q].get().unwrap();
        let b0 = self.base.get().unwrap()[q];
        let p = self.parent.get().unwrap();
        let best = self.best.get().unwrap();
        for (i, &k) in t.ids.iter().enumerate() {
            let r = find(p, b0 + i as u32) as usize;
            let mut cur = best[r].load(Ordering::Relaxed);
            while cur == 0 || id_cmp(k, cur).is_lt() {
                match best[r].compare_exchange_weak(cur, k, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
        }
    }

    fn stage_e(&self, q: usize) {
        let t = self.b[q].get().unwrap();
        let b0 = self.base.get().unwrap()[q];
        let p = self.parent.get().unwrap();
        let best = self.best.get().unwrap();
        let v: Vec<(K, K)> = t.ids.iter().enumerate().map(|(i, &k)| (k, best[find(p, b0 + i as u32) as usize].load(Ordering::Relaxed))).collect();
        *self.e[q].lock().unwrap() = v;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init() {
        let _ = T0.set(Instant::now());
    }

    fn rng(seed: u64) -> impl FnMut(u64) -> u64 {
        let mut x = seed | 1;
        move |m: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % m
        }
    }



    fn rand_id(r: &mut impl FnMut(u64) -> u64, i: u64) -> Vec<u8> {
        let sym = b"-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
        let shape = r(10);
        let n = match shape {
            0..=6 => 1 + r(10) as usize,
            7 | 8 => 11 + r(14) as usize,
            _ => 1 + r(14) as usize,
        };
        let mut s: Vec<u8> = (0..n).map(|_| sym[r(64) as usize]).collect();
        if shape == 9 {
            let j = r(n as u64) as usize;
            s[j] = [b'.', b'~', b'+', b' ', 0xC3][r(5) as usize];
        }

        if r(4) != 0 {
            s.extend_from_slice(format!("{}", i % 1000).as_bytes());
        }
        s
    }

    fn graph_csv(seed: u64) -> &'static [u8] {
        let mut r = rng(seed);
        let nid = 50 + r(3000);
        let ids: Vec<Vec<u8>> = (0..nid).map(|i| rand_id(&mut r, i)).collect();
        let mut edges: Vec<(usize, usize)> = Vec::new();
        let nidu = nid as usize;

        let mut k = 0usize;
        while k + 2 < nidu {
            let len = 1 + r(40) as usize;
            let end = (k + len).min(nidu - 1);
            for j in k..end {
                if r(2) == 0 { edges.push((j, j + 1)) } else { edges.push((j + 1, j)) }
            }
            if r(3) == 0 {
                edges.push((end, k));
            }
            k = end + 1 + r(3) as usize;
        }
        if nidu > 1700 && r(2) == 0 {
            let s = r((nidu - 1665) as u64) as usize;
            for j in s..s + 1664 {
                edges.push((s + r((j - s + 1) as u64) as usize, j + 1));
            }
        }
        for _ in 0..r(200) {
            let a = r(nid) as usize;
            edges.push((a, a));
        }
        for _ in 0..r(300) {
            edges.push((r(nid) as usize, r(nid) as usize));
        }
        let nd = r(200) as usize;
        for _ in 0..nd.min(edges.len()) {
            let e = edges[r(edges.len() as u64) as usize];
            edges.push(e);
        }

        for i in (1..edges.len()).rev() {
            let j = r(i as u64 + 1) as usize;
            edges.swap(i, j);
        }
        let mut v: Vec<u8> = b"left,right\n".to_vec();
        for (a, b) in edges {
            v.extend_from_slice(&ids[a]);
            v.push(b',');
            v.extend_from_slice(&ids[b]);
            v.push(b'\n');
        }
        if r(4) == 0 {
            v.extend_from_slice(b",x\nx,\nbad line\n");
        }
        if r(2) == 0 {
            v.pop();
        }
        Box::leak(v.into_boxed_slice())
    }

    fn sorted(parts: &[Vec<(K, K)>]) -> Vec<Vec<(K, K)>> {
        parts.iter().map(|p| { let mut p = p.clone(); p.sort_unstable(); p }).collect()
    }



    #[test]
    fn plinks_matches_serial() {
        init();
        for seed in 1..120u64 {
            let csv = graph_csv(seed * 0x9E37_79B9);
            let files: Vec<S> = if seed % 3 == 0 { vec![csv] } else { vec![csv, graph_csv(seed * 77 + 5)] };
            let np = [1usize, 2, 16, 64][(seed % 4) as usize];
            let serial = build_links(&files, np);
            let job = LinkJob::with_chunk(&files, np, [64usize, 1000, 4096, 1 << 20][(seed % 5 % 4) as usize]);
            let nt = 1 + (seed % 6) as usize;
            std::thread::scope(|s| {
                for _ in 0..nt {
                    s.spawn(|| job.run_to_end());
                }
            });
            let l = job.done().unwrap();
            assert_eq!(l.n, serial.n, "seed {seed}");
            assert_eq!(sorted(&l.parts), sorted(&serial.parts), "seed {seed}");
        }
    }

    #[test]
    fn plinks_small_chunks_and_empty() {
        init();
        let job = LinkJob::new(&[], 8);
        assert!(job.run_to_end().parts.iter().all(|p| p.is_empty()));
        assert_eq!(job.done().unwrap().n, 0);

        let h: S = b"a,b\n";
        let job = LinkJob::new(&[h], 4);
        assert_eq!(job.run_to_end().n, 0);
    }
}
