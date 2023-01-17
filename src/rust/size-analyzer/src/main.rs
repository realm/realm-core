use std::{
    fmt::Display,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Mutex,
};

use clap::Parser;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use realm_core::{
    sys::{ref_type, Array},
    *,
};

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
struct UnsafeSendable<'a, T> {
    value: &'a T,
}
unsafe impl<'a, T> Send for UnsafeSendable<'a, T> {}
unsafe impl<'a, T> Sync for UnsafeSendable<'a, T> {}
impl<'a, T> Deref for UnsafeSendable<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &*self.value
    }
}
impl<'a, T> UnsafeSendable<'a, T> {
    pub unsafe fn new(value: &'a T) -> Self {
        UnsafeSendable { value }
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
        results: &'env Mutex<Vec<(String, anyhow::Result<Box<dyn Display + Send>>)>>,
    ) {
        scope.spawn(move |_| {
            let result = analyzer.run(&txn);
            results.lock().unwrap().push((
                analyzer.name().to_string(),
                result.map(|a| Box::new(a) as _),
            ));
        });
    }

    let results: Vec<(String, anyhow::Result<Box<dyn Display + Send>>)> =
        thread_pool.install(|| {
            let results = Mutex::new(vec![]);

            rayon::scope(|scope| {
                if should_run(RunAnalysis::ArrayStats, &analyses) {
                    spawn_analyzer(&txn, ArrayStats, scope, &results);
                }

                // TODO: Add more analyzers.
            });

            results.into_inner().unwrap()
        });

    println!();
    println!("Results:");

    for (name, result) in results {
        println!();
        println!("{name}");
        println!("{}", str::repeat("=", name.len()));
        match result {
            Ok(result) => println!("{result}"),
            Err(err) => println!("ERROR: {err}"),
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ArrayWidths {
    /// Index is ceil(log2(bits + 1))
    pub counts: [usize; 8],
    /// 128+ bits
    pub larger: usize,
}

impl ArrayWidths {
    pub fn increment(&mut self, width: usize) {
        match index_for_bitwidth(width) {
            Some(index) => self.counts[index] += 1,
            None => self.larger += 1,
        }
    }
}

fn index_for_bitwidth(width: usize) -> Option<usize> {
    Some(match width {
        0 => 0,
        1 => 1,
        2 => 2,
        4 => 3,
        8 => 4,
        16 => 5,
        32 => 6,
        64 => 7,
        _ => return None,
    })
}

impl Display for ArrayWidths {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn print_it(
            f: &mut std::fmt::Formatter,
            bitwidth: impl Display,
            count: usize,
        ) -> std::fmt::Result {
            writeln!(f, "# arrays (width = {bitwidth: >4}): {count: >10}")
        }

        print_it(f, 0, self.counts[0])?;
        for (index, count) in self.counts[1..].into_iter().enumerate() {
            print_it(f, 1 << index, *count)?;
        }
        print_it(f, "128+", self.larger)?;

        Ok(())
    }
}

impl std::ops::Add for ArrayWidths {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let mut this = self;
        this += rhs;
        this
    }
}

impl std::ops::AddAssign for ArrayWidths {
    fn add_assign(&mut self, rhs: Self) {
        for (a, b) in self.counts.iter_mut().zip(rhs.counts.into_iter()) {
            *a += b;
        }
        self.larger += rhs.larger;
    }
}

#[derive(Debug, Default)]
pub struct Analysis {
    pub live_byte_size: u64,
    pub num_arrays: u64,
    pub array_length_sum: u64,
    pub arrays_with_refs: u64,
    pub arrays_bpnode: u64,
    pub widths: ArrayWidths,
    pub recursion_depth: usize,
}

impl Analysis {
    pub fn combine(mut self, other: Analysis) -> Self {
        self.live_byte_size += other.live_byte_size;
        self.num_arrays += other.num_arrays;
        self.array_length_sum += other.array_length_sum;
        self.arrays_with_refs += other.arrays_with_refs;
        self.arrays_bpnode += other.arrays_bpnode;
        self.widths += other.widths;
        self.recursion_depth = self.recursion_depth.max(other.recursion_depth);
        self
    }
}

impl Display for Analysis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Analysis {
            live_byte_size,
            num_arrays,
            array_length_sum,
            arrays_with_refs,
            arrays_bpnode,
            widths,
            recursion_depth,
        } = self;

        let average_array_length = *array_length_sum as f64 / *num_arrays as f64;

        writeln!(f, "Recursion depth:  {recursion_depth: >10}")?;
        writeln!(f, "Reachable bytes:  {live_byte_size: >10}")?;
        writeln!(f, "Num arrays:       {num_arrays: >10}")?;
        writeln!(f, "Avg array length: {average_array_length: >10.2}")?;
        writeln!(f, "Arrays with refs: {arrays_with_refs: >10}")?;
        writeln!(f, "Arrays (bpnodes): {arrays_bpnode: >10}")?;
        writeln!(f, "{widths}")?;

        Ok(())
    }
}

pub trait Analyzer: std::any::Any + Send {
    type Analysis: Send + Display;

    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    fn run(&mut self, txn: &FrozenTransaction) -> anyhow::Result<Self::Analysis>;
}

pub struct ArrayStats;

impl Analyzer for ArrayStats {
    type Analysis = Analysis;

    fn run(&mut self, txn: &FrozenTransaction) -> anyhow::Result<Self::Analysis> {
        let top_ref = txn.top_ref();
        let result = unsafe { analyze_ref(&txn, top_ref, 0)? };
        Ok(result)
    }
}

/// Recursive analysis of an array ref and its children.
///
/// SAFETY: The ref must be valid for the given allocator.
unsafe fn analyze_ref(
    txn: &FrozenTransaction,
    ref_: ref_type,
    recursion_depth: usize,
) -> anyhow::Result<Analysis> {
    let mut result = Analysis::default();
    let mut array = Array::new(txn.alloc())?;
    let mut array = array.as_mut().expect("NULL unique_ptr");
    array.as_mut().init_from_ref(ref_);

    result.num_arrays += 1;
    result.live_byte_size += array.get_byte_size() as u64;
    result.array_length_sum += array.size() as u64;
    result.recursion_depth = result.recursion_depth.max(recursion_depth);
    result.widths.increment(array.width_per_element_in_bits());
    if array.has_refs() {
        result.arrays_with_refs += 1;
    }
    if array.is_inner_bptree_node() {
        result.arrays_bpnode += 1;
    }

    // SAFETY: Transaction is frozen.
    let array = UnsafeSendable::new(&*array);

    let len = array.size();

    // Analyze children
    if array.has_refs() && len > 0 {
        if len > 1 {
            // Parallelize!
            let children_analysis = (0..len)
                .into_par_iter()
                .filter_map(|index| -> Option<anyhow::Result<_>> {
                    let value = array.get(index);
                    if is_ref(value) {
                        let ref_ = value as usize;
                        Some(analyze_ref(txn, ref_, recursion_depth + 1))
                    } else {
                        None
                    }
                })
                .reduce_with(|a, b| -> anyhow::Result<_> { Ok(a?.combine(b?)) })
                .transpose()?
                .unwrap_or_default();
            result = result.combine(children_analysis);
        } else {
            let value = array.get(0);
            if is_ref(value) {
                let ref_ = value as usize;
                let child_result = analyze_ref(txn, ref_, recursion_depth + 1)?;
                result = result.combine(child_result);
            }
        }
    }

    Ok(result)
}

fn is_ref(value: i64) -> bool {
    value != 0 && value & 1 == 0
}
