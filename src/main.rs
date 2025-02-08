mod schema;
mod size;

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

    let args = std::env::args();
    if args.len() <= 1 {
        panic!(
            "Please pass a command line argument with a directory containing JSON files to parse."
        );
    }

    for directory in args.skip(1) {
        eprintln!("Visiting directory: {directory:?}");
        visit_dirs(&directory, &mut |file_path| {
            let mut file = File::open(file_path)?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            total_input_bytes += data.len();

            let data: Result<schema::Data, _> = serde_json::from_slice(&data);
            let data = match data {
                Ok(data) => data,
                Err(err) => {
                    eprintln!("Error parsing JSON in file: {file_path:?}\n\t{err:?}");
                    file_error_count += 1;
                    return Ok(());
                }
            };
            total_parsed_bytes += data.estimated_bytes();

            file_count += 1;
            Ok(())
        })?;
    }

    println!("Parsed {total_input_bytes} bytes from {file_count} files (+ {file_error_count} failed files)");
    println!(
        "Expanded to {total_parsed_bytes} bytes in memory (relative size = {:.02}%)",
        total_parsed_bytes as f64 * 100.0 / total_input_bytes as f64,
    );

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
