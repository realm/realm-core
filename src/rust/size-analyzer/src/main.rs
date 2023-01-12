use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use clap::Parser;
use realm_core::*;

#[derive(Parser, Debug)]
struct Args {
    #[arg(value_name = "FILE")]
    file: PathBuf,
    #[arg(short, long)]
    generate_test_data: bool,
}

fn main() -> anyhow::Result<()> {
    let Args {
        file,
        generate_test_data,
    } = Args::parse();

    if generate_test_data {
        println!("Generating Realm file '{}'", file.display());
        generate_realm(&file)?;
        println!("Done");
    } else {
        println!("Analyzing Realm file '{}'", file.display());
        analyze_realm(&file)?;
        println!("Done");
    }

    Ok(())
}

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

fn analyze_realm(path: &Path) -> anyhow::Result<()> {
    use sys::{ref_type, Array};

    let db = DB::open(path, &Default::default())?;
    let txn = Transaction::new(&db)?;

    let alloc = txn.alloc();
    let top_ref = txn.top_ref();

    #[derive(Debug, Default)]
    struct Analysis {
        pub live_byte_size: u64,
        pub num_arrays: u64,
        pub array_length_sum: u64,
        pub arrays_with_refs: u64,
        pub arrays_bpnode: u64,
        pub widths: BTreeMap<usize, u64>,
        pub recursion_depth: usize,
    }

    /// SAFETY: The ref must be valid for the given allocator.
    unsafe fn analyze_ref(
        alloc: &sys::Allocator,
        ref_: ref_type,
        result: &mut Analysis,
        recursion_depth: usize,
    ) -> anyhow::Result<()> {
        let mut array = Array::new(alloc)?;
        let mut array = array.as_mut().expect("NULL unique_ptr");
        array.as_mut().init_from_ref(ref_);

        result.num_arrays += 1;
        result.live_byte_size += array.get_byte_size() as u64;
        result.array_length_sum += array.size() as u64;
        result.recursion_depth = result.recursion_depth.max(recursion_depth);
        *result
            .widths
            .entry(array.width_per_element_in_bits())
            .or_default() += 1;
        if array.has_refs() {
            result.arrays_with_refs += 1;
        }
        if array.is_inner_bptree_node() {
            result.arrays_bpnode += 1;
        }

        if array.has_refs() {
            let len = array.size();
            for i in 0..len {
                let value = array.get(i);
                if value != 0 && value & 1 == 0 {
                    // value is a ref
                    analyze_ref(alloc, value as _, result, recursion_depth + 1)?;
                }
            }
        }

        Ok(())
    }

    println!("Analyzing top ref: {top_ref}");
    let mut result = Analysis::default();
    unsafe {
        analyze_ref(alloc, top_ref, &mut result, 0)?;
    }

    let Analysis {
        live_byte_size,
        num_arrays,
        array_length_sum,
        arrays_with_refs,
        arrays_bpnode,
        widths,
        recursion_depth,
    } = result;

    let average_array_length = array_length_sum as f64 / num_arrays as f64;

    println!();
    println!("Results:");
    println!();
    println!("Recursion depth:  {recursion_depth: >10}");
    println!("Reachable bytes:  {live_byte_size: >10}");
    println!("Num arrays:       {num_arrays: >10}");
    println!("Avg array length: {average_array_length: >10.2}");
    println!("Arrays with refs: {arrays_with_refs: >10}");
    println!("Arrays (bpnodes): {arrays_bpnode: >10}");
    println!();

    for (width, count) in widths {
        println!("Arrays (width = {width: >3}): {count: >10}");
    }

    Ok(())
}
