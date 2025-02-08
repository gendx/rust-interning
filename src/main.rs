#![feature(iter_order_by)]

mod intern;
mod schema;
mod size;

use intern::EqWith;
use schema::optimized::Interners;
use size::EstimateSize;
use std::fmt::Debug;
use std::fs::{read_dir, DirEntry, File};
use std::io::Read;
use std::path::Path;

fn main() -> std::io::Result<()> {
    let mut file_count = 0;
    let mut file_error_count = 0;
    let mut total_input_bytes = 0;
    let mut total_parsed_bytes = 0;
    let mut total_optimized_bytes = 0;

    let args = std::env::args();
    if args.len() <= 1 {
        panic!(
            "Please pass a command line argument with a directory containing JSON files to parse."
        );
    }

    let mut interners = Interners::default();

    for directory in args.skip(1) {
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
