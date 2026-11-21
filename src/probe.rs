//! Scorer-environment probe (PROBE=1, set by the Dockerfile on the `probe` branch only).
//!
//! It is active only when the input looks like the public 1M-event dev set (events + clicks
//! under DEV_MAX_BYTES). On any other input `init` returns false, nothing is logged and the
//! pipeline runs exactly as without the probe, so nothing about hidden data is ever printed,
//! encoded or signalled (the exit code stays 0 either way).
//!
//! When active it prints the machine, sandbox, mount, THP, cgroup and page-cache facts plus a
//! few micro-benchmarks to stderr, turns on the TIMELINE stamps of the pipeline, and then
//! changes one campaign_daily counter so the dev check fails and the scorer returns the logs.

use std::sync::atomic::{AtomicBool, Ordering};

static ACTIVE: AtomicBool = AtomicBool::new(false);

/// events + clicks of the public dev set are ~90 MB; the practice and final data are >1 GB.
const DEV_MAX_BYTES: u64 = 256 << 20;

pub fn active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

/// Deliberately wrong output: one campaign_daily row gets one extra session.
pub fn spoil(rows: &mut [(&'static [u8], &'static [u8], i32, [i64; 4])]) {
    if active() {
        if let Some(r) = rows.first_mut() {
            r.3[0] += 1;
            p("PROBE spoiled campaign_daily row 0 (sessions + 1) so the dev check fails");
        }
    }
}

fn p(s: &str) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{}", s);
}

#[cfg(not(target_os = "linux"))]
pub fn init(_data: &std::path::Path) -> bool {
    false
}
#[cfg(not(target_os = "linux"))]
pub fn finish() {}

#[cfg(target_os = "linux")]
pub use linux::{finish, init};

#[cfg(target_os = "linux")]
mod linux {
    use super::{p, ACTIVE, DEV_MAX_BYTES};
    use std::os::unix::io::AsRawFd;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    fn now(clk: libc::clockid_t) -> f64 {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        unsafe { libc::clock_gettime(clk, &mut ts) };
        ts.tv_sec as f64 + ts.tv_nsec as f64 * 1e-9
    }

    fn rd(path: &str) -> String {
        std::fs::read_to_string(path).map(|s| s.trim_end().to_string()).unwrap_or_else(|e| format!("<{}>", e))
    }

    fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            let mut v: Vec<PathBuf> = rd.filter_map(|e| e.ok().map(|e| e.path())).collect();
            v.sort();
            for f in v {
                if f.is_dir() {
                    files_under(&f, out);
                } else {
                    out.push(f);
                }
            }
        }
    }

    fn dir_bytes(dir: &Path) -> u64 {
        let mut v = Vec::new();
        files_under(dir, &mut v);
        v.iter().filter_map(|f| std::fs::metadata(f).ok()).map(|m| m.len()).sum()
    }

    /// Process start (from /proc/self/stat field 22, clock ticks since boot) to now, in ms.
    fn exec_to_now_ms() -> (f64, f64) {
        let st = rd("/proc/self/stat");
        // fields after the ")" of comm; starttime is field 22 overall = index 19 after comm
        let after = st.rsplit_once(')').map(|x| x.1).unwrap_or("");
        let start_ticks: f64 = after.split_whitespace().nth(19).and_then(|x| x.parse().ok()).unwrap_or(0.0);
        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
        let boot = now(libc::CLOCK_BOOTTIME);
        ((boot - start_ticks / hz) * 1000.0, hz)
    }

    fn fs_type(path: &str) -> String {
        let c = std::ffi::CString::new(path).unwrap();
        let mut s: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(c.as_ptr(), &mut s) } != 0 {
            return "statfs failed".into();
        }
        let t = s.f_type as u64;
        let name = match t {
            0x794c7630 => "overlayfs",
            0x01021994 => "tmpfs",
            0xef53 => "ext2/3/4",
            0x65735546 => "fuse",
            0x01021997 => "v9fs(9p)",
            0x58465342 => "xfs",
            0x9123683e => "btrfs",
            0x6969 => "nfs",
            0x6a656a63 => "virtiofs?",
            0x62656572 => "sysfs",
            0x9fa0 => "proc",
            _ => "?",
        };
        format!("f_type 0x{:x} ({}) bsize {} blocks {} bfree {}", t, name, s.f_bsize, s.f_blocks, s.f_bfree)
    }

    fn uname() -> String {
        let mut u: libc::utsname = unsafe { std::mem::zeroed() };
        unsafe { libc::uname(&mut u) };
        let f = |a: &[libc::c_char]| unsafe { std::ffi::CStr::from_ptr(a.as_ptr()) }.to_string_lossy().into_owned();
        format!("{} {} {} {} {}", f(&u.sysname), f(&u.nodename), f(&u.release), f(&u.version), f(&u.machine))
    }

    fn resident(path: &Path) -> (u64, u64) {
        let Ok(f) = std::fs::File::open(path) else { return (0, 0) };
        let len = f.metadata().map(|m| m.len()).unwrap_or(0) as usize;
        if len == 0 {
            return (0, 0);
        }
        let pg = 4096usize;
        let ptr = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, f.as_raw_fd(), 0) };
        if ptr == libc::MAP_FAILED {
            return (0, 0);
        }
        let n = len.div_ceil(pg);
        let mut v = vec![0u8; n];
        let r = unsafe { libc::mincore(ptr, len, v.as_mut_ptr()) };
        unsafe { libc::munmap(ptr, len) };
        if r != 0 {
            return (0, n as u64);
        }
        (v.iter().filter(|&&b| b & 1 != 0).count() as u64, n as u64)
    }

    fn pread_pass(files: &[PathBuf], blk: usize, threads: usize) -> (f64, u64, u64) {
        let t = Instant::now();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let bytes = std::sync::atomic::AtomicU64::new(0);
        let calls = std::sync::atomic::AtomicU64::new(0);
        std::thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    let mut buf = vec![0u8; blk];
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= files.len() {
                            break;
                        }
                        let Ok(f) = std::fs::File::open(&files[i]) else { continue };
                        let mut off = 0i64;
                        loop {
                            let r = unsafe { libc::pread(f.as_raw_fd(), buf.as_mut_ptr() as *mut _, blk, off) };
                            calls.fetch_add(1, Ordering::Relaxed);
                            if r <= 0 {
                                break;
                            }
                            off += r as i64;
                        }
                        bytes.fetch_add(off as u64, Ordering::Relaxed);
                    }
                });
            }
        });
        (t.elapsed().as_secs_f64(), bytes.into_inner(), calls.into_inner())
    }

    fn fault_bench(label: &str, mb: usize, huge: bool) {
        let len = mb << 20;
        let al = 2 << 20;
        let raw = unsafe { libc::mmap(std::ptr::null_mut(), len + al, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0) };
        if raw == libc::MAP_FAILED {
            p(&format!("PROBE fault {} mmap failed", label));
            return;
        }
        let base = ((raw as usize + al - 1) & !(al - 1)) as *mut u8;
        unsafe { libc::madvise(base as *mut _, len, if huge { libc::MADV_HUGEPAGE } else { libc::MADV_NOHUGEPAGE }) };
        let f0 = super::super::minflt();
        let t = Instant::now();
        let mut i = 0;
        while i < len {
            unsafe { base.add(i).write_volatile(1) };
            i += 4096;
        }
        let t1 = t.elapsed().as_secs_f64();
        let f1 = super::super::minflt();
        let ahp = rd("/proc/self/smaps_rollup").lines().find(|l| l.starts_with("AnonHugePages")).unwrap_or("").split_whitespace().nth(1).unwrap_or("?").to_string();
        let t = Instant::now();
        let mut i = 0;
        while i < len {
            unsafe { base.add(i).write_volatile(2) };
            i += 4096;
        }
        let t2 = t.elapsed().as_secs_f64();
        let faults = (f1 - f0).max(1);
        p(&format!(
            "PROBE fault {:<6} {} MB: first touch {:.1} ms ({:.2} GB/s, {} minflt, {:.2} us/fault), second touch {:.1} ms, AnonHugePages {} kB",
            label, mb, t1 * 1e3, mb as f64 / 1024.0 / t1, faults, t1 * 1e6 / faults as f64, t2 * 1e3, ahp
        ));
        let t = Instant::now();
        unsafe { libc::munmap(raw, len + al) };
        p(&format!("PROBE fault {:<6} munmap {:.1} ms", label, t.elapsed().as_secs_f64() * 1e3));
    }

    fn populate_bench(mb: usize) {
        let len = mb << 20;
        let t = Instant::now();
        let raw = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE, -1, 0) };
        let t1 = t.elapsed().as_secs_f64();
        if raw != libc::MAP_FAILED {
            unsafe { libc::munmap(raw, len) };
        }
        p(&format!("PROBE MAP_POPULATE {} MB: {:.1} ms", mb, t1 * 1e3));
    }

    fn write_bench(dir: &str, mb: usize, fsync: bool) {
        let path = format!("{}/.probe_{}mb{}", dir, mb, if fsync { "_fsync" } else { "" });
        let buf: Vec<u8> = (0..(1usize << 20)).map(|i| (i * 2654435761usize >> 13) as u8).collect();
        let t = Instant::now();
        let f = match std::fs::File::create(&path) {
            Ok(f) => f,
            Err(e) => {
                p(&format!("PROBE write {} {} MB: create failed: {}", dir, mb, e));
                return;
            }
        };
        let tc = t.elapsed().as_secs_f64();
        for _ in 0..mb {
            let _ = unsafe { libc::write(f.as_raw_fd(), buf.as_ptr() as *const _, buf.len()) };
        }
        let tw = t.elapsed().as_secs_f64();
        if fsync {
            unsafe { libc::fsync(f.as_raw_fd()) };
        }
        let ts = t.elapsed().as_secs_f64();
        drop(f);
        let tcl = t.elapsed().as_secs_f64();
        let _ = std::fs::remove_file(&path);
        let tu = t.elapsed().as_secs_f64();
        p(&format!(
            "PROBE write {:<8} {:>2} MB{}: create {:.2} ms, write {:.1} ms ({:.2} GB/s), fsync {:.1} ms, close {:.2} ms, unlink {:.2} ms",
            dir, mb, if fsync { " +fsync" } else { "" }, tc * 1e3, (tw - tc) * 1e3, mb as f64 / 1024.0 / (tw - tc), (ts - tw) * 1e3, (tcl - ts) * 1e3, (tu - tcl) * 1e3
        ));
    }

    fn syscall_bench() {
        let n = 100_000;
        let t = Instant::now();
        for _ in 0..n {
            unsafe { libc::syscall(libc::SYS_getpid) };
        }
        let a = t.elapsed().as_secs_f64() * 1e9 / n as f64;
        let t = Instant::now();
        let mut x = 0.0;
        for _ in 0..n {
            x += now(libc::CLOCK_MONOTONIC);
        }
        let b = t.elapsed().as_secs_f64() * 1e9 / n as f64;
        let t = Instant::now();
        for _ in 0..n {
            x += now(libc::CLOCK_THREAD_CPUTIME_ID);
        }
        let c = t.elapsed().as_secs_f64() * 1e9 / n as f64;
        let fd = unsafe { libc::open(c"/proc/self/stat".as_ptr(), libc::O_RDONLY) };
        let t = Instant::now();
        let mut ob = [0u8; 8];
        for _ in 0..n {
            unsafe { libc::pread(fd, ob.as_mut_ptr() as *mut _, 0, 0) };
        }
        let d = t.elapsed().as_secs_f64() * 1e9 / n as f64;
        unsafe { libc::close(fd) };
        let t = Instant::now();
        for _ in 0..1000 {
            let h = std::thread::spawn(|| 1u32);
            let _ = h.join();
        }
        let e = t.elapsed().as_secs_f64() * 1e6 / 1000.0;
        p(&format!(
            "PROBE syscall: getpid {:.0} ns, clock_gettime(MONOTONIC) {:.0} ns, clock_gettime(THREAD_CPUTIME) {:.0} ns, pread(0 B) {:.0} ns, thread spawn+join {:.1} us (x={})",
            a, b, c, d, e, x as u64 & 1
        ));
    }

    fn spin_bench() {
        // single-thread ALU loop, then the same on all CPUs (frequency / SMT / neighbour signal)
        fn spin(iters: u64) -> u64 {
            let mut h = 0x9e3779b97f4a7c15u64;
            for i in 0..iters {
                h = (h ^ i).wrapping_mul(0xbf58476d1ce4e5b9).rotate_left(17);
            }
            h
        }
        let iters = 50_000_000u64;
        let t = Instant::now();
        let h = spin(iters);
        let one = t.elapsed().as_secs_f64();
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let t = Instant::now();
        let hs: Vec<u64> = std::thread::scope(|s| {
            let v: Vec<_> = (0..n).map(|_| s.spawn(move || spin(iters))).collect();
            v.into_iter().map(|j| j.join().unwrap()).collect()
        });
        let all = t.elapsed().as_secs_f64();
        p(&format!(
            "PROBE spin: 1 thread {:.1} ms ({:.2} ns/iter), {} threads {:.1} ms (x{:.2} throughput) [{}]",
            one * 1e3, one * 1e9 / iters as f64, n, all * 1e3, n as f64 * one / all, (h ^ hs.iter().fold(0, |a, b| a ^ b)) & 1
        ));
    }

    /// Returns true when the probe is active (dev-sized input and PROBE set).
    pub fn init(data: &Path) -> bool {
        if std::env::var_os("PROBE").is_none() {
            return false;
        }
        let (exec_ms, hz) = exec_to_now_ms();
        let main_wall = now(libc::CLOCK_REALTIME);
        let size = dir_bytes(&data.join("events")) + dir_bytes(&data.join("clicks"));
        if size == 0 || size > DEV_MAX_BYTES {
            return false; // not the dev set: behave exactly like the normal build, print nothing
        }
        ACTIVE.store(true, Ordering::Relaxed);
        let t0 = Instant::now();
        p("PROBE ================= scorer environment probe (dev set only) =================");
        p(&format!("PROBE exec->probe init {:.1} ms (CLK_TCK {}, so +-{:.0} ms), realtime {:.3}", exec_ms, hz, 1000.0 / hz, main_wall));
        p(&format!("PROBE uname -a: {}", uname()));
        p(&format!("PROBE /proc/version: {}", rd("/proc/version")));
        p(&format!("PROBE /proc/cmdline: {}", rd("/proc/cmdline")));
        p(&format!("PROBE uptime: {} | loadavg: {}", rd("/proc/uptime"), rd("/proc/loadavg")));
        p(&format!("PROBE clocksource: {} (available: {})", rd("/sys/devices/system/clocksource/clocksource0/current_clocksource"), rd("/sys/devices/system/clocksource/clocksource0/available_clocksource")));
        for l in rd("/proc/self/mountinfo").lines() {
            p(&format!("PROBE mountinfo: {}", l));
        }
        for d in ["/", "/data", "/output", "/scratch", "/tmp", "/app"] {
            p(&format!("PROBE statfs {:<8} {}", d, fs_type(d)));
        }
        for f in ["enabled", "defrag", "khugepaged/defrag", "shmem_enabled", "hpage_pmd_size"] {
            p(&format!("PROBE THP {}: {}", f, rd(&format!("/sys/kernel/mm/transparent_hugepage/{}", f))));
        }
        // CPU
        let ci = rd("/proc/cpuinfo");
        let first = ci.split("\n\n").next().unwrap_or("");
        for l in first.lines() {
            let k = l.split(':').next().unwrap_or("").trim();
            if matches!(k, "vendor_id" | "cpu family" | "model" | "model name" | "stepping" | "microcode" | "cpu MHz" | "cache size" | "physical id" | "siblings" | "core id" | "cpu cores" | "bogomips" | "bugs" | "address sizes") {
                p(&format!("PROBE cpu0 {}", l.replace('\t', "")));
            }
            if k == "flags" {
                let fl: Vec<&str> = l.split(':').nth(1).unwrap_or("").split_whitespace().collect();
                let avx: Vec<&str> = fl.iter().copied().filter(|f| f.starts_with("avx") || f.starts_with("amx") || matches!(*f, "hypervisor" | "bmi2" | "vaes" | "vpclmulqdq" | "gfni" | "sha_ni" | "erms" | "fsrm" | "pdpe1gb" | "constant_tsc" | "nonstop_tsc" | "tsc_known_freq" | "ht")).collect();
                p(&format!("PROBE cpu0 flags ({} total) selected: {}", fl.len(), avx.join(" ")));
                p(&format!("PROBE cpu0 flags all: {}", fl.join(" ")));
            }
        }
        let mhz: Vec<&str> = ci.lines().filter(|l| l.starts_with("cpu MHz")).map(|l| l.split(':').nth(1).unwrap_or("").trim()).collect();
        p(&format!("PROBE cpu MHz per cpu ({} cpus listed): {}", mhz.len(), mhz.join(" ")));
        p(&format!("PROBE cpu online {} present {} possible {}", rd("/sys/devices/system/cpu/online"), rd("/sys/devices/system/cpu/present"), rd("/sys/devices/system/cpu/possible")));
        let mut sib = Vec::new();
        for c in 0..64 {
            let s = std::fs::read_to_string(format!("/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list", c));
            match s {
                Ok(s) => sib.push(format!("{}:{}", c, s.trim())),
                Err(_) => break,
            }
        }
        p(&format!("PROBE thread_siblings_list: {}", sib.join(" ")));
        for i in 0..5 {
            let b = format!("/sys/devices/system/cpu/cpu0/cache/index{}", i);
            if Path::new(&b).exists() {
                p(&format!("PROBE cache L{} {} {} shared {}", rd(&format!("{}/level", b)), rd(&format!("{}/type", b)), rd(&format!("{}/size", b)), rd(&format!("{}/shared_cpu_list", b))));
            }
        }
        let nodes = std::fs::read_dir("/sys/devices/system/node").map(|r| r.filter_map(|e| e.ok()).filter(|e| e.file_name().to_string_lossy().starts_with("node")).count()).unwrap_or(0);
        p(&format!("PROBE numa nodes: {}", nodes));
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
        let aff: Vec<String> = (0..1024).filter(|&c| unsafe { libc::CPU_ISSET(c, &set) }).map(|c| c.to_string()).collect();
        p(&format!("PROBE sched_getaffinity: [{}] ({}), available_parallelism {:?}, nprocs_onln {}", aff.join(","), aff.len(), std::thread::available_parallelism().map(|n| n.get()), unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) }));
        let st = rd("/proc/self/status");
        for l in st.lines() {
            let k = l.split(':').next().unwrap_or("");
            if matches!(k, "Uid" | "CapEff" | "CapBnd" | "NoNewPrivs" | "Seccomp" | "Seccomp_filters" | "Cpus_allowed_list" | "Mems_allowed_list" | "THP_enabled") {
                p(&format!("PROBE status {}", l.replace('\t', " ")));
            }
        }
        p(&format!("PROBE /proc/self/cgroup: {}", rd("/proc/self/cgroup").replace('\n', " | ")));
        for f in ["cpu.max", "cpu.weight", "cpuset.cpus.effective", "cpuset.mems.effective", "memory.max", "memory.high", "memory.swap.max", "memory.current", "pids.max", "cpu.stat", "io.max"] {
            p(&format!("PROBE cgroup {}: {}", f, rd(&format!("/sys/fs/cgroup/{}", f)).replace('\n', " | ")));
        }
        for f in ["cpu.cfs_quota_us", "cpu.cfs_period_us", "memory.limit_in_bytes"] {
            let a = format!("/sys/fs/cgroup/cpu/{}", f);
            let b = format!("/sys/fs/cgroup/memory/{}", f);
            if Path::new(&a).exists() {
                p(&format!("PROBE cgroup-v1 {}: {}", f, rd(&a)));
            }
            if Path::new(&b).exists() {
                p(&format!("PROBE cgroup-v1 {}: {}", f, rd(&b)));
            }
        }
        for l in rd("/proc/meminfo").lines().filter(|l| ["MemTotal", "MemFree", "MemAvailable", "Buffers", "Cached", "Shmem:", "AnonHugePages", "HugePages_Total", "Hugepagesize", "Dirty"].iter().any(|k| l.starts_with(k))) {
            p(&format!("PROBE meminfo {}", l));
        }
        for f in ["cpu", "memory", "io"] {
            p(&format!("PROBE pressure {}: {}", f, rd(&format!("/proc/pressure/{}", f)).replace('\n', " | ")));
        }
        let stat = rd("/proc/stat");
        p(&format!("PROBE /proc/stat cpu: {}", stat.lines().next().unwrap_or("")));
        let mut env: Vec<String> = std::env::vars().map(|(k, v)| format!("{}={}", k, v)).collect();
        env.sort();
        p(&format!("PROBE env: {}", env.join(" ")));
        p(&format!("PROBE exe: {:?} {} bytes", std::env::current_exe().ok(), std::env::current_exe().ok().and_then(|e| std::fs::metadata(e).ok()).map(|m| m.len()).unwrap_or(0)));

        // page cache residency of every /data file, before anything reads it
        let mut files = Vec::new();
        files_under(data, &mut files);
        let (mut rs, mut tot) = (0u64, 0u64);
        let mut per = Vec::new();
        for f in &files {
            let (r, n) = resident(f);
            rs += r;
            tot += n;
            per.push(format!("{}:{}/{}", f.strip_prefix(data).unwrap_or(f).display(), r, n));
        }
        p(&format!("PROBE mincore /data total: {} of {} pages resident ({:.1}%), {} files", rs, tot, 100.0 * rs as f64 / tot.max(1) as f64, files.len()));
        for ch in per.chunks(12) {
            p(&format!("PROBE mincore {}", ch.join(" ")));
        }

        // pread throughput over the dev files: first pass (cold if not resident), then warm
        let big: Vec<PathBuf> = files.iter().filter(|f| f.extension().map(|e| e == "csv").unwrap_or(false)).cloned().collect();
        for (lbl, blk, th) in [("pass1 1MB x1", 1usize << 20, 1usize), ("pass2 1MB x1", 1 << 20, 1), ("pass3 16MB x1", 16 << 20, 1), ("pass4 1MB x8", 1 << 20, 8), ("pass5 16MB x8", 16 << 20, 8)] {
            let (t, b, c) = pread_pass(&big, blk, th);
            p(&format!("PROBE pread {:<14}: {} MB in {:.1} ms = {:.2} GB/s, {} calls ({:.1} us/call)", lbl, b >> 20, t * 1e3, b as f64 / 1e9 / t, c, t * 1e6 / c.max(1) as f64));
        }
        let (r2, t2) = files.iter().map(|f| resident(f)).fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1));
        p(&format!("PROBE mincore /data after preads: {} of {} resident", r2, t2));

        syscall_bench();
        fault_bench("4k", 256, false);
        fault_bench("thp", 256, true);
        populate_bench(256);
        spin_bench();
        for d in ["/output", "/scratch", "/tmp"] {
            write_bench(d, 10, false);
            if d == "/output" {
                write_bench(d, 50, false);
                write_bench(d, 50, true);
            }
        }
        p(&format!("PROBE probe benches took {:.1} ms; pipeline follows with TIMELINE stamps (TL <epoch ms> [s since main] ...)", t0.elapsed().as_secs_f64() * 1e3));
        true
    }

    pub fn finish() {
        if !super::active() {
            return;
        }
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
        p(&format!(
            "PROBE rusage: utime {:.3} s, stime {:.3} s, maxrss {} MB, minflt {}, majflt {}, nvcsw {}, nivcsw {}, inblock {}, oublock {}",
            ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 * 1e-6,
            ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 * 1e-6,
            ru.ru_maxrss >> 10, ru.ru_minflt, ru.ru_majflt, ru.ru_nvcsw, ru.ru_nivcsw, ru.ru_inblock, ru.ru_oublock
        ));
        p(&format!("PROBE schedstat (run ns, wait ns, slices): {}", rd("/proc/self/schedstat")));
        for l in rd("/proc/self/smaps_rollup").lines().filter(|l| l.starts_with("Rss") || l.starts_with("AnonHugePages")) {
            p(&format!("PROBE smaps_rollup {}", l));
        }
        p(&format!("PROBE io: {}", rd("/proc/self/io").replace('\n', " | ")));
        p(&format!("PROBE cgroup cpu.stat: {}", rd("/sys/fs/cgroup/cpu.stat").replace('\n', " | ")));
        p(&format!("PROBE cgroup memory.peak: {}", rd("/sys/fs/cgroup/memory.peak")));
        p(&format!("PROBE pressure cpu: {}", rd("/proc/pressure/cpu").replace('\n', " | ")));
        p(&format!("PROBE loadavg: {} | /proc/stat cpu: {}", rd("/proc/loadavg"), rd("/proc/stat").lines().next().unwrap_or("")));
        let (e, _) = exec_to_now_ms();
        p(&format!("PROBE exec->exit {:.1} ms, realtime {:.3}", e, now(libc::CLOCK_REALTIME)));
        p("PROBE ================= end of probe (output deliberately wrong) =================");
    }
}
