// pmbench - Poor Man's benchmarking tools
// Copyright (C) 2026  Maxim Petrov
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{
    fs::{File, OpenOptions},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use libc;

pub fn run(args: &[String]) -> Result<(), String> {
    run_wrapper(args).map_err(|e| format!("ddsimple: {e}"))
}

fn run_wrapper(args: &[String]) -> Result<(), String> {
    if args.len() != 4 {
        return Err("usage: BDEV_PATH DATA_DIRECTION BLOCKSIZE NR_THREADS".to_string());
    }

    let bdev = PathBuf::from_str(&args[0])
        .map_err(|e| format!("failed to create path from arg {}: {e}", args[0]))?;
    let is_write = match args[1].as_str() {
        "w" | "write" => true,
        "r" | "read" => false,
        other => return Err(format!("bad data direction '{other}'")),
    };
    let blocksize: u64 = args[2]
        .parse()
        .map_err(|e| format!("bad blocksize '{}': {e}", args[2]))?;
    let nr_threads: usize = args[3]
        .parse()
        .map_err(|e| format!("bad number of threads '{}': {e}", args[3]))?;

    do_run(bdev, is_write, blocksize, nr_threads)
}

fn do_run(bdev: PathBuf, is_write: bool, blocksize: u64, nr_threads: usize) -> Result<(), String> {
    if !blocksize.is_multiple_of(4096) {
        return Err(format!(
            "bad blocksize, should be multiple of 4096, but got {blocksize}"
        ));
    }

    if nr_threads == 0 {
        return Err("number of threads should be at least 1".to_string());
    }

    let stats: Vec<Arc<AtomicU64>> = (0..nr_threads).map(|_| Arc::default()).collect();
    let active_threads: AtomicUsize = AtomicUsize::new(nr_threads);

    let file = OpenOptions::new()
        .custom_flags(libc::O_DIRECT)
        .read(!is_write)
        .write(is_write)
        .open(&bdev)
        .map_err(|e| format!("failed to open {bdev:?}: {e}"))?;

    thread::scope(|s| {
        let active = &active_threads;

        // spawn workers
        for (i, stat) in stats.iter().enumerate() {
            let file = file.try_clone().unwrap();
            let stat = stat.clone();
            s.spawn(move || do_thread(i, nr_threads, file, blocksize, is_write, stat, active));
        }

        // spawn statistics thread
        s.spawn(|| {
            const SLEEP_PERIOD: Duration = Duration::from_millis(500);
            let mut last_time = Instant::now();
            let mut last_stats = vec![0u64; nr_threads];
            while active_threads.load(Ordering::Acquire) != 0 {
                thread::sleep(SLEEP_PERIOD);

                let curr_stats: Vec<u64> =
                    stats.iter().map(|s| s.load(Ordering::Relaxed)).collect();
                let curr_time = Instant::now();

                let dt = (curr_time - last_time).as_secs_f64();
                for (i, (last, curr)) in last_stats.iter_mut().zip(curr_stats.iter()).enumerate() {
                    let nr_ios = *curr - *last;
                    let kiops = nr_ios as f64 / dt / 1e3;
                    *last = *curr;

                    println!("{i}: {kiops} kIO/s");
                }

                last_time = curr_time;
            }
        });
    });

    Ok(())
}

fn do_thread(
    tid: usize,
    nr_threads: usize,
    file: File,
    blocksize: u64,
    is_write: bool,
    stat: Arc<AtomicU64>,
    active: &AtomicUsize,
) {
    let mut buffer = vec![0u8; blocksize as usize + 4095];
    let buffer = {
        let mismatch = buffer.as_ptr() as usize % 4096;
        if mismatch == 0 {
            &mut buffer[0..blocksize as usize]
        } else {
            &mut buffer[(4096 - mismatch)..(4096 - mismatch + blocksize as usize)]
        }
    };
    assert_eq!(buffer.as_ptr() as usize % 4096, 0);

    loop {
        let counter = stat.fetch_add(1, Ordering::Relaxed);
        let offset = (counter * nr_threads as u64 + tid as u64) * blocksize;

        if is_write {
            match file.write_at(buffer, offset) {
                Ok(size) => {
                    if (size as u64) != blocksize {
                        eprintln!("thread {tid} write: {size} != {blocksize}");
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("thread {tid} write: {e}, stopping");
                    break;
                }
            }
        } else {
            match file.read_at(buffer, offset) {
                Ok(size) => {
                    if (size as u64) != blocksize {
                        eprintln!("thread {tid} read: {size} != {blocksize}");
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("thread {tid} read: {e}, stopping");
                    break;
                }
            }
        }
    }
    active.fetch_sub(1, Ordering::Release);
}
