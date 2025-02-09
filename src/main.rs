#![feature(iter_order_by)]

mod intern;
mod schema;
mod size;

use intern::EqWith;
use schema::optimized::Interners;
use serde::{Deserialize, Serialize};
use size::EstimateSize;
use std::fmt::Debug;
use std::fs::{read_dir, DirEntry, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut file_count = 0;
    let mut file_error_count = 0;
    let mut total_input_bytes = 0;
    let mut total_parsed_bytes = 0;
    let mut total_optimized_bytes = 0;

    let mut args = std::env::args();
    if args.len() <= 2 {
        panic!(
            "Please pass a command line argument with (1) an output directory and (2) one or more directori(es) containing JSON files to parse."
        );
    }

    let mut interners = Interners::default();
    let mut datas = Vec::new();

    args.next(); // Ignoring the program path.
    let output_dir: PathBuf = args.next().unwrap().into();
    for directory in args {
        eprintln!("Visiting directory: {directory:?}");
        visit_dirs(&directory, &mut |file_path| {
            let mut file = File::open(file_path)?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            total_input_bytes += data.len();

            let data: Result<schema::source::Data, _> = serde_json::from_slice(&data);
            let data = match data {
                Ok(data) => data,
                Err(err) => {
                    eprintln!("Error parsing JSON in file: {file_path:?}\n\t{err:?}");
                    file_error_count += 1;
                    return Ok(());
                }
            };
            total_parsed_bytes += data.estimated_bytes();

            let optimized = schema::optimized::Data::from(&mut interners, data.clone());
            total_optimized_bytes += optimized.estimated_bytes();

            assert!(
                optimized.eq_with(&data, &interners),
                "Optimized data didn't match original for file: {file_path:?}"
            );

            datas.push(optimized);

            file_count += 1;
            Ok(())
        })?;
    }

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

    println!("+---------------+-------------------+");
    println!("|    Format     |       Bytes       |");
    println!("+---------------+-----------+-------+");
    bincode_bytes.print_sizes("Bincode", total_input_bytes);
    cbor_bytes.print_sizes("CBOR", total_input_bytes);
    json_bytes.print_sizes("JSON", total_input_bytes);
    json_pretty_bytes.print_sizes("JSON (pretty)", total_input_bytes);
    postcard_bytes.print_sizes("Postcard", total_input_bytes);
    println!("+---------------+---------+-+-------+");
    println!("|               |   enc   |   dec   |");
    println!("+---------------+---------+---------+");
    bincode_bytes.print_times("Bincode");
    cbor_bytes.print_times("CBOR");
    json_bytes.print_times("JSON");
    json_pretty_bytes.print_times("JSON (pretty)");
    postcard_bytes.print_times("Postcard");
    println!("+---------------+---------+---------+");

    Ok(())
}

fn visit_dirs(
    dir: impl AsRef<Path> + Debug,
    callback: &mut impl FnMut(&Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    eprintln!("Reading directory: {dir:?}");

    // Sort entries by path for reproducibility.
    let mut entries: Vec<DirEntry> = read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_unstable_by_key(|x| x.path());
    for entry in entries {
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
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Database {
    interners: Interners,
    datas: Vec<schema::optimized::Data>,
}

struct CodecStats {
    encoded_size: usize,
    encode_time: Duration,
    decode_time: Duration,
}

impl CodecStats {
    fn print_sizes(&self, title: &str, total_bytes: usize) {
        println!(
            "| {title:<13} | {:>9} | {:.02}% |",
            self.encoded_size,
            self.encoded_size as f64 * 100.0 / total_bytes as f64,
        );
    }

    fn print_times(&self, title: &str) {
        println!(
            "| {title:<13} |{:>5} ms |{:>5} ms |",
            self.encode_time.as_millis(),
            self.decode_time.as_millis(),
        );
    }
}

fn serde_round_trip<T: PartialEq + Debug>(
    t: &T,
    path: impl AsRef<Path> + Debug,
    serialize: impl FnOnce(&T) -> Result<Vec<u8>, Box<dyn std::error::Error>>,
    deserialize: impl FnOnce(&[u8]) -> Result<T, Box<dyn std::error::Error>>,
) -> Result<CodecStats, Box<dyn std::error::Error>> {
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

    Ok(CodecStats {
        encoded_size: serialized.len(),
        encode_time,
        decode_time,
    })
}
