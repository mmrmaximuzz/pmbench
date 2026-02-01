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
    collections::HashMap,
    io::Read,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, Sender, TryRecvError},
    },
    thread,
    time::Duration,
};

use chrono;

pub fn run(args: &[String]) -> Result<(), String> {
    run_wrapper(args).map_err(|e| format!("iperfsimple error: {e}"))
}

enum Mode {
    Receiver,
    Sender,
}

enum Protocol {
    Tcp,
    Udp,
}

fn run_wrapper(args: &[String]) -> Result<(), String> {
    if args.len() < 3 {
        return Err("usage: (recv|send) (tcp|udp) ENDPOINT ..".to_string());
    }

    let mode = match args[0].as_str() {
        "recv" => Mode::Receiver,
        "send" => Mode::Sender,
        other => {
            return Err(format!(
                "unsupported mode '{other}': must be 'recv' or 'send'"
            ));
        }
    };

    let proto = match args[1].as_str() {
        "tcp" => Protocol::Tcp,
        "udp" => Protocol::Udp,
        other => {
            return Err(format!(
                "unsupported proto '{other}': must be 'tcp' or 'udp'"
            ));
        }
    };

    let endpoint = args[2].clone();

    match (mode, proto) {
        (Mode::Receiver, Protocol::Tcp) => {
            run_tcp_receiver(endpoint).map_err(|e| format!("tcp receiver error: {e}"))
        }
        (Mode::Receiver, Protocol::Udp) => {
            run_udp_receiver(endpoint).map_err(|e| format!("udp receiver error: {e}"))
        }
        (Mode::Sender, Protocol::Tcp) => todo!(),
        (Mode::Sender, Protocol::Udp) => todo!(),
    }
}

enum StatsAction {
    Add(SocketAddr, Arc<(AtomicU64, AtomicU64)>),
    Del(SocketAddr),
}

fn run_tcp_receiver(endpoint: String) -> Result<(), String> {
    let server =
        TcpListener::bind(endpoint).map_err(|e| format!("TCP server failed to bind: {e}"))?;

    let (tx, rx) = mpsc::channel();

    thread::spawn(move || statistics_thread(rx));

    loop {
        let (client, addr) = server
            .accept()
            .map_err(|e| format!("TCP server failed to accept connection: {e}"))?;

        let tx = tx.clone();
        thread::spawn(move || tcp_receiver_thread(client, addr, tx));
    }
}

fn tcp_receiver_thread(mut client: TcpStream, addr: SocketAddr, tx: Sender<StatsAction>) {
    let mut buffer = [0u8; 65536];
    let stats = Arc::new((AtomicU64::default(), AtomicU64::default()));

    // notifier statistics thread that we are starting
    tx.send(StatsAction::Add(addr, stats.clone())).unwrap();

    loop {
        match client.read(&mut buffer) {
            Err(e) => {
                panic!("error from thread '{addr}': {e}");
            }
            Ok(0) => break, // completed
            Ok(s) => {
                stats.0.fetch_add(1, Ordering::Relaxed);
                stats.1.fetch_add(s as u64, Ordering::Relaxed);
            }
        }
    }

    tx.send(StatsAction::Del(addr)).unwrap();
}

fn statistics_thread(rx: Receiver<StatsAction>) {
    let mut stats = HashMap::new();
    let mut last_time = chrono::Local::now();
    loop {
        thread::sleep(Duration::from_millis(500));

        // first check new stats
        loop {
            match rx.try_recv() {
                Ok(StatsAction::Add(addr, stat)) => {
                    stats.insert(addr, (stat, 0u64, 0u64));
                }
                Ok(StatsAction::Del(addr)) => {
                    stats.remove(&addr);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }

        let curr_time = chrono::Local::now();
        let delta_time = (curr_time - last_time).as_seconds_f64();
        last_time = curr_time;
        let timestring = curr_time.to_rfc3339_opts(chrono::SecondsFormat::Micros, false);

        for (addr, (stat, old_msgs, old_bytes)) in stats.iter_mut() {
            let new_msgs = stat.0.load(Ordering::Relaxed);
            let new_bytes = stat.1.load(Ordering::Relaxed);
            let delta_msgs = new_msgs - *old_msgs;
            let delta_bytes = new_bytes - *old_bytes;
            *old_msgs = new_msgs;
            *old_bytes = new_bytes;

            let kpps = delta_msgs as f64 / delta_time * 1e-3;
            let mbps = delta_bytes as f64 / delta_time * 8e-6;
            println!("{addr}:{timestring}:{kpps},{mbps}");
        }
    }
}

fn run_udp_receiver(_endpoint: String) -> Result<(), String> {
    todo!()
}
