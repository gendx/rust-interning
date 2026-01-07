#![feature(exit_status_error, iter_order_by)]

mod intern;
mod schema;
mod size;

use intern::EqWith;
use rayon::prelude::*;
use schema::optimized::Interners;
use serde::{Deserialize, Serialize};
use size::EstimateSize;
use std::fmt::Debug;
use std::fs::{read_dir, DirEntry, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_count = AtomicUsize::new(0);
    let file_error_count = AtomicUsize::new(0);
    let total_input_bytes = AtomicUsize::new(0);
    let total_parsed_bytes = AtomicUsize::new(0);
    let total_optimized_bytes = AtomicUsize::new(0);

    let mut args = std::env::args();
    if args.len() <= 2 {
        panic!(
            "Please pass a command line argument with (1) an output directory and (2) one or more directori(es) containing JSON files to parse."
        );
    }

    let interners = Interners::default();
    let datas = Mutex::new(Vec::new());

    args.next(); // Ignoring the program path.
    let output_dir: PathBuf = args.next().unwrap().into();
    for directory in args {
        eprintln!("Visiting directory: {directory:?}");
        visit_dirs(&directory, &|file_path| {
            let mut file = File::open(file_path)?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            total_input_bytes.fetch_add(data.len(), Ordering::Relaxed);

            let data: Result<schema::source::Data, _> = serde_json::from_slice(&data);
            let data = match data {
                Ok(data) => data,
                Err(err) => {
                    eprintln!("Error parsing JSON in file: {file_path:?}\n\t{err:?}");
                    file_error_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
            };
            total_parsed_bytes.fetch_add(data.estimated_bytes(), Ordering::Relaxed);

            let optimized = schema::optimized::Data::from(&interners, data.clone());
            total_optimized_bytes.fetch_add(optimized.estimated_bytes(), Ordering::Relaxed);

            assert!(
                optimized.eq_with(&data, &interners),
                "Optimized data didn't match original for file: {file_path:?}"
            );

            datas.lock().unwrap().push(optimized);

            file_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })?;
    }

    let file_count = file_count.load(Ordering::Relaxed);
    let file_error_count = file_error_count.load(Ordering::Relaxed);
    let total_input_bytes = total_input_bytes.load(Ordering::Relaxed);
    let total_parsed_bytes = total_parsed_bytes.load(Ordering::Relaxed);
    let mut total_optimized_bytes = total_optimized_bytes.load(Ordering::Relaxed);
    let datas = datas.into_inner().unwrap();

    println!("Parsed {total_input_bytes} bytes from {file_count} files (+ {file_error_count} failed files)");
    println!(
        "Expanded to {total_parsed_bytes} bytes in memory (relative size = {:.02}%)",
        total_parsed_bytes as f64 * 100.0 / total_input_bytes as f64,
    );

    let interners_bytes = interners.estimated_bytes();
    total_optimized_bytes += interners_bytes;
    println!(
        "Optimized to {total_optimized_bytes} bytes (relative size = {:.02}%)",
        total_optimized_bytes as f64 * 100.0 / total_input_bytes as f64,
    );
    println!(
        "[{:.02}%] Interners: {interners_bytes} bytes",
        interners_bytes as f64 * 100.0 / total_optimized_bytes as f64,
    );
    interners.print_summary(total_optimized_bytes);

    let database = Database { interners, datas };
    eprintln!("Serializing database into directory: {output_dir:?}");

    let bincode_bytes = serde_round_trip(
        &database,
        output_dir.join("bincode.db"),
        |value| Ok(bincode::serialize(value)?),
        |bytes| Ok(bincode::deserialize(bytes)?),
    )?;

    let cbor_bytes = serde_round_trip(
        &database,
        output_dir.join("cbor.db"),
        |value| {
            let mut output = Vec::new();
            ciborium::into_writer(value, &mut output)?;
            Ok(output)
        },
        |bytes| Ok(ciborium::from_reader(bytes)?),
    )?;

    let json_bytes = serde_round_trip(
        &database,
        output_dir.join("json.db"),
        |value| Ok(serde_json::to_vec(value)?),
        |bytes| Ok(serde_json::from_slice(bytes)?),
    )?;

    let json_pretty_bytes = serde_round_trip(
        &database,
        output_dir.join("json_pretty.db"),
        |value| Ok(serde_json::to_vec_pretty(value)?),
        |bytes| Ok(serde_json::from_slice(bytes)?),
    )?;

    let postcard_bytes = serde_round_trip(
        &database,
        output_dir.join("postcard.db"),
        |value| Ok(postcard::to_stdvec(value)?),
        |bytes| Ok(postcard::from_bytes(bytes)?),
    )?;

    println!("+---------------+-------------------+-------------------+-------------------+-------------------+");
    println!("|    Format     |       Bytes       |      gzip -6      |       xz -6       |     brotli -6     |");
    println!("+---------------+-----------+-------+-----------+-------+-----------+-------+-----------+-------+");
    bincode_bytes.print_sizes("Bincode", total_input_bytes);
    cbor_bytes.print_sizes("CBOR", total_input_bytes);
    json_bytes.print_sizes("JSON", total_input_bytes);
    json_pretty_bytes.print_sizes("JSON (pretty)", total_input_bytes);
    postcard_bytes.print_sizes("Postcard", total_input_bytes);
    println!("+---------------+---------+-+-------+---------+-+-------+---------+-+-------+---------+-+-------+");
    println!("|               |   enc   |   dec   |   enc   |   dec   |   enc   |   dec   |   enc   |   dec   |");
    println!("+---------------+---------+---------+---------+---------+---------+---------+---------+---------+");
    bincode_bytes.print_times("Bincode");
    cbor_bytes.print_times("CBOR");
    json_bytes.print_times("JSON");
    json_pretty_bytes.print_times("JSON (pretty)");
    postcard_bytes.print_times("Postcard");
    println!("+---------------+---------+---------+---------+---------+---------+---------+---------+---------+");

    Ok(())
}

fn visit_dirs(
    dir: impl AsRef<Path> + Debug,
    callback: &(impl Fn(&Path) -> std::io::Result<()> + Sync),
) -> std::io::Result<()> {
    eprintln!("Reading directory: {dir:?}");

    // Sort entries by path for reproducibility.
    let mut entries: Vec<DirEntry> = read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_unstable_by_key(|x| x.path());
    entries
        .par_iter()
        .try_for_each(|entry| -> std::io::Result<()> {
            let mut path = entry.path();
            let mut file_type = entry.file_type()?;

            // Resolve symbolic links.
            if file_type.is_symlink() {
                eprint!("Resolving symlink: {path:?}");
                path = std::fs::canonicalize(path)?;
                eprintln!(" -> {path:?}");
                file_type = path.metadata()?.file_type();
            }

            if file_type.is_dir() {
                visit_dirs(path, callback)?;
            } else if file_type.is_file() {
                callback(&path)?;
            } else {
                eprintln!("Skipping path of unknown file type {file_type:?}: {path:?}");
            }

            Ok(())
        })?;

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Database {
    interners: Interners,
    datas: Vec<schema::optimized::Data>,
}

struct Stats {
    serialized: CodecStats,
    gzip: CodecStats,
    xz: CodecStats,
    brotli: CodecStats,
}

struct CodecStats {
    encoded_size: usize,
    encode_time: Duration,
    decode_time: Duration,
}

impl Stats {
    fn print_sizes(&self, title: &str, total_bytes: usize) {
        println!(
            "| {title:<13} | {:>9} | {:.02}% | {:>9} | {:.02}% | {:>9} | {:.02}% | {:>9} | {:.02}% |",
            self.serialized.encoded_size,
            self.serialized.encoded_size as f64 * 100.0 / total_bytes as f64,
            self.gzip.encoded_size,
            self.gzip.encoded_size as f64 * 100.0 / total_bytes as f64,
            self.xz.encoded_size,
            self.xz.encoded_size as f64 * 100.0 / total_bytes as f64,
            self.brotli.encoded_size,
            self.brotli.encoded_size as f64 * 100.0 / total_bytes as f64,
        );
    }

    fn print_times(&self, title: &str) {
        println!(
            "| {title:<13} |{:>5} ms |{:>5} ms |{:>5} ms |{:>5} ms |{:>5} ms |{:>5} ms |{:>5} ms |{:>5} ms |",
            self.serialized.encode_time.as_millis(),
            self.serialized.decode_time.as_millis(),
            self.gzip.encode_time.as_millis(),
            self.gzip.decode_time.as_millis(),
            self.xz.encode_time.as_millis(),
            self.xz.decode_time.as_millis(),
            self.brotli.encode_time.as_millis(),
            self.brotli.decode_time.as_millis(),
        );
    }
}

fn serde_round_trip<T: PartialEq + Debug>(
    t: &T,
    path: impl AsRef<Path> + Debug,
    serialize: impl FnOnce(&T) -> Result<Vec<u8>, Box<dyn std::error::Error>>,
    deserialize: impl FnOnce(&[u8]) -> Result<T, Box<dyn std::error::Error>>,
) -> Result<Stats, Box<dyn std::error::Error>> {
    eprintln!("- Serializing to: {path:?}");

    eprint!("Serializing...");
    let start = Instant::now();
    let serialized = serialize(t)?;
    let encode_time = Instant::now().duration_since(start);
    eprintln!(
        " {:?} | {:.02} MB/s",
        encode_time,
        serialized.len() as f64 / (1_000_000.0 * encode_time.as_secs_f64()),
    );

    eprint!("Deserializing...");
    let start = Instant::now();
    let deserialized = deserialize(&serialized)?;
    let decode_time = Instant::now().duration_since(start);
    eprintln!(
        " {:?} | {:.02} MB/s",
        decode_time,
        serialized.len() as f64 / (1_000_000.0 * decode_time.as_secs_f64()),
    );

    assert_eq!(&deserialized, t);

    let mut f = File::create(path)?;
    f.write_all(&serialized)?;
    drop(f);

    Ok(Stats {
        serialized: CodecStats {
            encoded_size: serialized.len(),
            encode_time,
            decode_time,
        },
        gzip: gzip_round_trip(&serialized)?,
        xz: xz_round_trip(&serialized)?,
        brotli: brotli_round_trip(&serialized)?,
    })
}

fn gzip_round_trip(bytes: &[u8]) -> Result<CodecStats, Box<dyn std::error::Error>> {
    codec_round_trip(
        "gzip",
        bytes,
        || {
            let mut command = Command::new("gzip");
            command.arg("-c").arg("-6");
            command
        },
        || {
            let mut command = Command::new("gzip");
            command.arg("-c").arg("-d");
            command
        },
    )
}

fn xz_round_trip(bytes: &[u8]) -> Result<CodecStats, Box<dyn std::error::Error>> {
    codec_round_trip(
        "xz",
        bytes,
        || {
            let mut command = Command::new("xz");
            command.arg("-c").arg("-6");
            command
        },
        || {
            let mut command = Command::new("xz");
            command.arg("-c").arg("-d");
            command
        },
    )
}

fn brotli_round_trip(bytes: &[u8]) -> Result<CodecStats, Box<dyn std::error::Error>> {
    codec_round_trip(
        "brotli",
        bytes,
        || {
            let mut command = Command::new("brotli");
            command.arg("-c").arg("-6");
            command
        },
        || {
            let mut command = Command::new("brotli");
            command.arg("-c").arg("-d");
            command
        },
    )
}

fn codec_round_trip(
    title: &str,
    bytes: &[u8],
    compress: impl FnOnce() -> Command,
    decompress: impl FnOnce() -> Command,
) -> Result<CodecStats, Box<dyn std::error::Error>> {
    eprint!("[{title}] Compressing {} bytes...", bytes.len());
    let start = Instant::now();
    let compressed: Vec<u8> = io_command(
        compress()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?,
        bytes,
    )?;
    let encode_time = Instant::now().duration_since(start);
    eprintln!(
        " {:?} | {:.02} MB/s",
        encode_time,
        compressed.len() as f64 / (1_000_000.0 * encode_time.as_secs_f64()),
    );

    // Decompress to validate that compression worked properly.
    eprint!("[{title}] Decompressing {} bytes...", compressed.len());
    let start = Instant::now();
    let decompressed: Vec<u8> = io_command(
        decompress()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?,
        &compressed,
    )?;
    let decode_time = Instant::now().duration_since(start);
    eprintln!(
        " {:?} | {:.02} MB/s",
        decode_time,
        compressed.len() as f64 / (1_000_000.0 * decode_time.as_secs_f64()),
    );

    assert_eq!(decompressed, bytes);
    Ok(CodecStats {
        encoded_size: compressed.len(),
        encode_time,
        decode_time,
    })
}

fn io_command(mut child: Child, input: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    thread::scope(|s| -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut stdin = child.stdin.take().expect("Failed to open stdin");
        let mut stdout = child.stdout.take().expect("Failed to open stdout");

        let input_thread = s.spawn(move || -> std::io::Result<()> {
            stdin.write_all(input)?;
            drop(stdin);
            Ok(())
        });

        let output_thread = s.spawn(move || -> std::io::Result<Vec<u8>> {
            let mut output = Vec::new();
            stdout.read_to_end(&mut output)?;
            Ok(output)
        });

        child.wait()?.exit_ok()?;

        input_thread.join().expect("Failed to join input thread")?;
        let output = output_thread
            .join()
            .expect("Failed to join output thread")?;
        Ok(output)
    })
}
