





use super::*;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8};

pub const M40: u64 = (1 << 40) - 1;
const C1: u64 = 0x9E37_79B9_7F4B;
const C2: u64 = 0xD6E8_FEB8_6659;
pub const FAIL: u64 = u64::MAX;

fn inv40(a: u64) -> u64 {
    let mut x: u64 = a;
    for _ in 0..6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
    }
    x & M40
}

#[derive(Clone, Copy)]
pub struct Un {
    c1i: u64,
    c2i: u64,
    key: u64,
}

impl Un {
    pub fn new(key: u64) -> Un {
        Un { c1i: inv40(C1), c2i: inv40(C2), key: key & M40 }
    }

    #[inline(always)]
    fn pre(&self, z: u64) -> u64 {
        let y = z.wrapping_mul(self.c2i) & M40;
        y ^ (y >> 17) ^ (y >> 34)
    }
    #[inline(always)]
    fn fin(&self, x: u64, key: u64) -> u64 {
        ((x ^ key).wrapping_mul(self.c1i)) & M40
    }
    #[inline(always)]
    pub fn un(&self, z: u64) -> u64 {
        self.fin(self.pre(z), self.key)
    }

    pub fn fwd(&self, n: u64) -> u64 {
        let mut m = ((n & M40).wrapping_mul(C1) ^ self.key) & M40;
        m ^= m >> 17;
        m.wrapping_mul(C2) & M40
    }
}


static PAIR: std::sync::OnceLock<Vec<u16>> = std::sync::OnceLock::new();
pub fn pair_table() -> &'static [u16] {
    PAIR.get_or_init(|| {
        let dig = |c: u32| -> Option<u32> {
            match c as u8 {
                b'0'..=b'9' => Some(c - b'0' as u32),
                b'a'..=b'z' => Some(c - b'a' as u32 + 10),
                _ => None,
            }
        };
        (0..1u32 << 14)
            .map(|i| match (dig(i >> 7), dig(i & 127)) {
                (Some(a), Some(b)) => (a * 36 + b) as u16,
                _ => 0x8000,
            })
            .collect()
    })
}


const fn padb() -> [u64; 9] {
    let mut t = [0u64; 9];
    let mut m = 0;
    while m <= 8 {
        let mut p = 0u64;
        let mut j = 0;
        while j < 8 - m {
            p |= 0x30u64 << (8 * (7 - j));
            j += 1;
        }
        t[m] = p;
        m += 1;
    }
    t
}
static PADB: [u64; 9] = padb();



#[inline(always)]
pub fn parse36(s: &[u8], lim: usize, prefix: u8, t: &[u16]) -> u64 {
    let n = s.len();
    let m = n.wrapping_sub(1);
    if m.wrapping_sub(1) >= 8 || s[0] != prefix {
        return FAIL;
    }
    let p = s.as_ptr();
    let raw = if p as usize + 9 <= lim {
        u64::from_be(unsafe { std::ptr::read_unaligned(p.add(1) as *const u64) })
    } else {
        let mut b = [0u8; 8];
        b[..m].copy_from_slice(&s[1..]);
        u64::from_be_bytes(b)
    };
    let x = (raw >> (8 * (8 - m))) | PADB[m];
    let lead0 = ((x >> (8 * (m - 1))) & 0xFF == 0x30) & (m > 1);
    let i = |sh: u32| ((((x >> (sh + 8)) & 0x7F) << 7) | ((x >> sh) & 0x7F)) as usize;

    let a = unsafe { *t.get_unchecked(i(48)) } as u64;
    let b = unsafe { *t.get_unchecked(i(32)) } as u64;
    let c = unsafe { *t.get_unchecked(i(16)) } as u64;
    let d = unsafe { *t.get_unchecked(i(0)) } as u64;
    let v = (a * 1296 + b) * 1_679_616 + (c * 1296 + d);
    let bad = (((a | b | c | d) & 0x8000) != 0) | lead0 | (v > M40) | (x & 0x8080_8080_8080_8080 != 0);
    if bad { FAIL } else { v }
}


#[inline(always)]
pub fn dec(s: &[u8], lim: usize, prefix: u8, un: &Un, t: &[u16]) -> u64 {
    let z = parse36(s, lim, prefix, t);
    if z == FAIL { FAIL } else { un.un(z) }
}


pub fn enc(n: u64, prefix: u8, un: &Un, out: &mut [u8; 9]) -> usize {
    const DIG: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut z = un.fwd(n);
    let mut buf = [0u8; 8];
    let mut at = 8;
    loop {
        at -= 1;
        buf[at] = DIG[(z % 36) as usize];
        z /= 36;
        if z == 0 {
            break;
        }
    }
    out[0] = prefix;
    let l = 8 - at;
    out[1..1 + l].copy_from_slice(&buf[at..]);
    1 + l
}


#[inline]
pub fn ukey_of(u: u64, un: &Un) -> K {
    let mut b = [0u8; 9];
    let n = enc(u, b'u', un, &mut b);

    kfast_s(unsafe { std::mem::transmute::<&[u8], S>(&b[..n]) }).expect("encoded ids are fast-shape")
}





pub fn recover(ev: &[(S, S, Option<i64>)]) -> Option<(u64, u64)> {
    let t = pair_table();
    let z0 = Un::new(0);
    const UB: u64 = 1 << 35;
    const EB: u64 = 1 << 32;


    let mut cnt: hashbrown::HashMap<S, u32, foldhash::fast::FixedState> = Default::default();
    for e in ev {
        if !e.1.is_empty() {
            *cnt.entry(e.1).or_default() += 1;
        }
    }
    let mut ids: Vec<(u32, S)> = cnt.iter().map(|(&s, &c)| (c, s)).collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));
    let ux: Vec<u64> = ids.iter().filter_map(|&(_, s)| {
        let z = parse36(s, 0, b'u', t);
        (z != FAIL).then(|| z0.pre(z))
    }).collect();
    if ux.len() < 17 {
        return None;
    }
    let step = (ux.len() / 16).max(1);
    let uver: Vec<u64> = ux[1..].iter().step_by(step).take(16).copied().collect();
    let search = |x0: u64, ver: &[u64], bound: u64, lim: u64| -> Option<u64> {
        let mut vc: u64 = 0;
        for _ in 0..lim {
            let k = (x0 ^ vc) & M40;
            vc = vc.wrapping_add(C1) & M40;
            if z0.fin(ver[0], k) >= bound {
                continue;
            }
            if ver.iter().filter(|&&y| z0.fin(y, k) < bound).count() * 8 >= ver.len() * 7 {
                return Some(k);
            }
        }
        None
    };
    let uk = search(ux[0], &uver, UB, 1 << 20)?;
    let uu = Un::new(uk);


    let pers = |s: S| -> Option<u64> {
        let v = dec(s, 0, b'u', &uu, t);
        (v < UB).then_some(v >> 9)
    };
    let pmin = ev.iter().filter_map(|e| pers(e.1)).min()?;
    let mut rows: Vec<(i64, u64)> = ev.iter().filter(|e| pers(e.1) == Some(pmin)).filter_map(|e| {
        let z = parse36(e.0, 0, b'e', t);
        Some((e.2?, if z == FAIL { return None } else { z0.pre(z) }))
    }).collect();
    rows.sort_unstable();
    let ex: Vec<u64> = ev.iter().filter_map(|e| {
        let z = parse36(e.0, 0, b'e', t);
        (z != FAIL).then(|| z0.pre(z))
    }).collect();
    if ex.len() < 17 {
        return None;
    }
    let ever: Vec<u64> = ex.iter().step_by((ex.len() / 16).max(1)).take(16).copied().collect();
    let ek = search(rows.first()?.1, &ever, EB, 1 << 25)?;

    let ue = Un::new(ek);
    let ok_u = ev.iter().filter(|e| dec(e.1, 0, b'u', &uu, t) < UB).count();
    let ok_e = ev.iter().filter(|e| dec(e.0, 0, b'e', &ue, t) < EB).count();
    if ok_u * 100 < ev.len() * 95 || ok_e * 100 < ev.len() * 99 {
        return None;
    }
    Some((uk, ek))
}


pub fn zatom<T>(n: usize) -> &'static [T] {
    let bytes = (n * std::mem::size_of::<T>()).max(64);
    let p = huge_region(bytes);
    unsafe { std::slice::from_raw_parts(p as *const T, n) }
}

pub const DIRTY: u8 = 1;
pub const MIXED: u8 = 2;
pub const CLS_COMP: u32 = 0xFFFF;


pub struct Dz {
    pub uu: Un,
    pub ue: Un,
    pub pb: u64,
    pub eb: u64,
    pub lgs: u32,
    pub npd: usize,
    pub cps: u32,
    pub ncp: usize,
    pub flags: &'static [AtomicU8],


    pub mp: &'static [[AtomicU64; 2]],
    pub seg: &'static [AtomicU32],
    pub cls: &'static [AtomicU32],
}

impl Dz {

    #[inline(always)]
    pub fn user(&self, s: &[u8], lim: usize, t: &[u16]) -> u64 {
        let v = dec(s, lim, b'u', &self.uu, t);
        if v < self.pb << 9 { v } else { FAIL }
    }
    #[inline(always)]
    pub fn event(&self, s: &[u8], lim: usize, t: &[u16]) -> u64 {
        let v = dec(s, lim, b'e', &self.ue, t);
        if v < self.eb { v } else { FAIL }
    }
    #[inline(always)]
    pub fn class(&self, u: u32) -> u32 {
        let (p, a) = ((u >> 9) as usize, u & 511);
        if a < 64 && (self.mp[p][0].load(Ordering::Relaxed) >> a) & 1 == 1 { CLS_COMP } else { a }
    }
    #[inline(always)]
    pub fn dense(&self, p: usize) -> bool {
        self.flags[p].load(Ordering::Relaxed) == 0 && self.seg[p].load(Ordering::Relaxed) == 1
    }

    pub fn user_of_key(&self, k: K, t: &[u16]) -> u64 {
        if k >> 63 == 1 {
            let mut b = [0u8; 9];
            let n = fast_bytes(k, &mut b);
            return self.user(&b[..n], 0, t);
        }
        if k == 0 {
            return FAIL;
        }
        let mut b: Vec<u8> = Vec::with_capacity(32);
        id_bytes(k, &mut b);
        self.user(&b, 0, t)
    }



    pub fn link_part(&self, part: &[(K, K)], t: &[u16]) {
        for &(k, pk) in part {
            let dk = self.user_of_key(k, t);
            let dp = self.user_of_key(pk, t);
            let dirty = |p: u64| {
                self.flags[p as usize].fetch_or(DIRTY, Ordering::Relaxed);
            };
            if dk != FAIL {
                let p = dk >> 9;
                if dp == FAIL || dp >> 9 != p {
                    dirty(p);
                    if dp != FAIL {
                        dirty(dp >> 9);
                    }
                } else {
                    let a = dk & 511;
                    if a < 64 {
                        self.mp[p as usize][0].fetch_or(1 << a, Ordering::Relaxed);
                    } else {
                        dirty(p);
                    }
                    if let Err(old) = self.mp[p as usize][1].compare_exchange(0, pk, Ordering::Relaxed, Ordering::Relaxed) {
                        if old != pk {
                            dirty(p);
                        }
                    }
                }
            } else if dp != FAIL {
                dirty(dp >> 9);
            }
        }
    }
}


pub fn setup(ev_paths: &[PathBuf], total_ev: usize) -> Option<Dz> {
    if std::env::var_os("NODENSE").is_some() {
        return None;
    }

    let mut bufs: Vec<&'static [u8]> = Vec::new();
    for p in ev_paths.iter().take(8) {
        use std::io::Read;
        let mut f = std::fs::File::open(p).ok()?;
        let mut b = vec![0u8; 256 << 10];
        let mut n = 0;
        while n < b.len() {
            match f.read(&mut b[n..]) {
                Ok(0) => break,
                Ok(k) => n += k,
                Err(_) => break,
            }
        }
        b.truncate(n);

        if let Some(e) = b.iter().rposition(|&c| c == b'\n') {
            b.truncate(e + 1);
        }
        bufs.push(Box::leak(b.into_boxed_slice()));
        if bufs.iter().map(|b| b.len()).sum::<usize>() >= 512 << 10 {
            break;
        }
    }
    let mut rows: Vec<(S, S, Option<i64>)> = Vec::new();
    for d in &bufs {
        let mut f: [S; 7] = [b""; 7];
        let mut first = true;
        for_lines(d, 0, d.len(), |l| {
            if first {
                first = false;
                return;
            }
            if rows.len() < 4096 && split(l, &mut f) {
                rows.push((f[0], f[1], parse_ts(f[2])));
            }
        });
        if rows.len() >= 4096 {
            break;
        }
    }
    if rows.len() < 256 {
        return None;
    }
    let (uk, ek) = recover(&rows)?;
    let uu = Un::new(uk);
    let ue = Un::new(ek);
    let t = pair_table();

    let maxp = rows.iter().map(|r| dec(r.1, 0, b'u', &uu, t)).filter(|&v| v < 1 << 35).map(|v| v >> 9).max()?;
    let maxe = rows.iter().map(|r| dec(r.0, 0, b'e', &ue, t)).filter(|&v| v < 1 << 32).max()?;
    let pb = ((maxp + 1) * 2).next_power_of_two().min(1 << 23);
    let eb = ((maxe + 1) * 2).next_power_of_two().min(1 << 31).max((total_ev as u64 / 64).next_power_of_two().min(1 << 31));
    let lgeb = 63 - eb.leading_zeros();
    let lgs = lgeb.saturating_sub(11).max(12).min(lgeb);
    let lgs = std::env::var("DLGS").ok().and_then(|v| v.parse::<u32>().ok()).filter(|&v| v >= 8 && v <= lgeb && lgeb - v <= 14).unwrap_or(lgs);
    let npd = (eb >> lgs) as usize;
    let lgpb = 63 - pb.leading_zeros();
    let cps = lgpb.saturating_sub(9);
    let ncp = (pb >> cps) as usize;
    let n = pb as usize;
    Some(Dz { uu, ue, pb, eb, lgs, npd, cps, ncp, flags: zatom(n), mp: zatom(n), seg: zatom(n), cls: zatom(n) })
}
