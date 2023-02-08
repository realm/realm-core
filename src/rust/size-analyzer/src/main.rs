mod bitscan;

use std::{
    fmt::Display,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Mutex,
};

use clap::Parser;
use realm_core::*;

mod array_stats;
mod string_compression;

#[derive(Parser, Debug)]
struct Args {
    #[arg(value_name = "FILE")]
    file: PathBuf,
    #[arg(short, long)]
    generate_test_data: bool,
    #[arg(short, long, default_value = "1")]
    num_threads: u8,
    #[arg(
        short,
        long,
        default_value = "all",
        use_value_delimiter = true,
        value_delimiter = ','
    )]
    analyses: Vec<RunAnalysis>,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
enum RunAnalysis {
    All,
    ArrayStats,
    StringCompressionByTable,
    StringCompressionByLeaf,
}

pub type Analysis = Box<dyn Display + Send + Sync>;

pub trait Analyzer: std::any::Any + Send {
    fn name(&self) -> String {
        std::any::type_name::<Self>().into()
    }

    fn run(&mut self, txn: &FrozenTransaction) -> anyhow::Result<Analysis>;
}

fn should_run(analysis: RunAnalysis, analyses: &[RunAnalysis]) -> bool {
    analyses
        .iter()
        .any(|a| *a == analysis || *a == RunAnalysis::All)
}

fn main() -> anyhow::Result<()> {
    let Args {
        file,
        generate_test_data,
        num_threads,
        analyses,
    } = Args::parse();

    if num_threads == 0 {
        anyhow::bail!("Invalid number of threads (must be positive nonzero)");
    }

    if generate_test_data {
        println!("Generating Realm file '{}'", file.display());
        generate_realm(&file)?;
        println!("Done");
    } else {
        println!(
            "Analyzing Realm file '{}' (with {num_threads} thread(s))",
            file.display()
        );
        analyze_realm(&file, num_threads, &analyses)?;
        println!("Done");
    }

    Ok(())
}

/// Generate a trivial Realm with two tables.
fn generate_realm(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        anyhow::bail!("File already exists: {}", path.display());
    }

    const LOREM_IPSUM: &'static str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    let db = DB::open(path, &Default::default())?;
    let txn = Transaction::new(&db)?;
    let mut writer = txn.promote_to_write()?;

    {
        let table1 = writer.add_table("class_Table1", sys::TableType::TopLevel)?;
        let int_col = table1.add_column(&mut writer, DataType::Int, "int", false)?;
        let str_col = table1.add_column(&mut writer, DataType::String, "string", false)?;

        for i in 0..100_000 {
            let obj = table1.create_object()?;
            obj.set_int(int_col, 100 * i)?;
            obj.set_string(str_col, LOREM_IPSUM)?;
        }
    }

    {
        let table2 = writer.add_table("class_Table2", sys::TableType::TopLevel)?;
        let int_col = table2.add_column(&mut writer, DataType::Int, "int", false)?;

        for i in 0..100_000 {
            let obj = table2.create_object()?;
            obj.set_int(int_col, 1000 * i)?;
        }
    }

    writer.commit()?;

    Ok(())
}

/// Wrapper around a type that can be `Send + Sync` under particular conditions,
/// like an accessor that belongs to a frozen transaction.
#[derive(Clone, Copy, Debug)]
struct UnsafeSendable<T> {
    value: T,
}
unsafe impl<T> Send for UnsafeSendable<T> {}
unsafe impl<T> Sync for UnsafeSendable<T> {}
impl<T> Deref for UnsafeSendable<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl<T> UnsafeSendable<T> {
    pub unsafe fn new(value: T) -> Self {
        UnsafeSendable { value }
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

fn analyze_realm(path: &Path, num_threads: u8, analyses: &[RunAnalysis]) -> anyhow::Result<()> {
    let db = DB::open(path, &Default::default())?;
    let txn = Transaction::new(&db)?.freeze()?;

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads as _)
        .build()?;

    fn spawn_analyzer<'env, A: Analyzer>(
        txn: &'env FrozenTransaction,
        mut analyzer: A,
        scope: &rayon::Scope<'env>,
        results: &'env Mutex<Vec<(String, anyhow::Result<Analysis>)>>,
    ) {
        scope.spawn(move |_| {
            let result = analyzer.run(&txn);
            results
                .lock()
                .unwrap()
                .push((analyzer.name().to_string(), result));
        });
    }

    let mut results: Vec<(String, anyhow::Result<Analysis>)> = thread_pool.install(|| {
        let results = Mutex::new(vec![]);

        rayon::scope(|scope| {
            if should_run(RunAnalysis::ArrayStats, &analyses) {
                spawn_analyzer(&txn, array_stats::ArrayStats, scope, &results);
            }

            if should_run(RunAnalysis::StringCompressionByLeaf, &analyses) {
                spawn_analyzer(
                    &txn,
                    string_compression::StringCompressionByLeaf {
                        dict_size: 1024,
                        num_samples: 16,
                    },
                    scope,
                    &results,
                );
                spawn_analyzer(
                    &txn,
                    string_compression::StringCompressionByLeaf {
                        dict_size: 512,
                        num_samples: 16,
                    },
                    scope,
                    &results,
                );
                spawn_analyzer(
                    &txn,
                    string_compression::StringCompressionByLeaf {
                        dict_size: 2048,
                        num_samples: 16,
                    },
                    scope,
                    &results,
                );
            }

            if should_run(RunAnalysis::StringCompressionByTable, &analyses) {
                eprintln!("string-compression-by-table not implemented yet");
            }

            // TODO: Add more analyzers.
        });

        results.into_inner().unwrap()
    });

    println!();
    println!("Results:");

    results.sort_by(|a, b| a.0.cmp(&b.0));

    for (name, result) in results {
        println!();
        println!("{name}");
        println!("{}", str::repeat("=", name.len()));
        match result {
            Ok(result) => println!("{result}"),
            Err(err) => println!("ERROR: {err:?}"),
        }
    }

    Ok(())
}
