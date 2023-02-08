use core::num;
use std::{fmt::Display, io::Write};

use anyhow::Context;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use realm_core::{sys::ArrayString, FrozenTransaction, TableRef};

use crate::{Analyzer, UnsafeSendable};

pub struct StringCompressionByLeaf {
    pub dict_size: usize,
    pub num_samples: usize,
}

// Zstd dictionary training requires at least 8 samples.
const MIN_SAMPLES: usize = 8;

impl Analyzer for StringCompressionByLeaf {
    fn name(&self) -> String {
        format!(
            "{} (dict_size = {}, num_samples = {})",
            std::any::type_name::<Self>(),
            self.dict_size,
            self.num_samples
        )
    }

    fn run(&mut self, txn: &realm_core::FrozenTransaction) -> anyhow::Result<crate::Analysis> {
        // For each table, for each string column, for each leaf:
        //
        // - Train a new dictionary.
        // - Compress all strings in the leaf using the dictionary.
        // - Sum the new column size in the leaf.

        let table_keys = txn.get_table_keys();
        let tables = table_keys
            .into_iter()
            .map(|key| {
                txn.get_table(key).map(|table| unsafe {
                    // SAFETY: Transaction is frozen.
                    UnsafeSendable::new(table)
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .context("get table keys")?;

        // Find all string columns (as a tuple of the table ref and the index of
        // the string column).
        let string_columns: Vec<(UnsafeSendable<TableRef>, usize)> = tables
            .into_iter()
            .flat_map(|table| {
                let spec = table.spec();
                let column_count = spec.get_column_count();
                (0..column_count)
                    .filter(|index| {
                        spec.get_column_type(*index) == realm_core::sys::ColumnType::String
                    })
                    .map(|index| (table, index))
                    .collect::<Vec<_>>()
            })
            .collect();

        let num_samples = self.num_samples.max(MIN_SAMPLES);

        let result = string_columns
            .into_par_iter()
            .map(|(table, column_index)| {
                analyze_column(
                    txn,
                    table.into_inner(),
                    column_index,
                    self.dict_size,
                    num_samples,
                )
            })
            .try_reduce_with(|a, mut b| Ok(a.combine(&mut b)));

        match result {
            Some(result) => Ok(Box::new(result.context("string compression by leaf")?)),
            None => Ok(Box::new(StringCompressionByLeafResult::default())),
        }
    }
}

fn analyze_column(
    txn: &FrozenTransaction,
    table: TableRef,
    column_index: usize,
    dict_size: usize,
    num_samples: usize,
) -> anyhow::Result<StringCompressionByLeafResult> {
    assert_eq!(
        table.spec().get_column_type(column_index),
        realm_core::sys::ColumnType::String
    );

    let mut leaf_refs = vec![];

    table.traverse_clusters(|cluster| {
        let column_ref = cluster.get_column_ref(column_index);
        leaf_refs.push(column_ref);
        realm_core::sys::IteratorControl::AdvanceToNext
    })?;

    let result = leaf_refs
        .into_par_iter()
        .map(|leaf_ref| analyze_leaf(txn, leaf_ref, dict_size, num_samples))
        .try_reduce_with(|a, mut b| Ok(a.combine(&mut b)));

    result.unwrap_or(Ok(Default::default()))
}

fn analyze_leaf(
    txn: &FrozenTransaction,
    leaf_ref: realm_core::sys::ref_type,
    dict_size: usize,
    num_samples: usize,
) -> anyhow::Result<StringCompressionByLeafResult> {
    let mut array = ArrayString::new(txn.alloc())?;
    let mut array = array.as_mut().expect("NULL unique_ptr");
    unsafe {
        // SAFETY: Ref comes from the cluster.
        array.as_mut().init_from_ref(leaf_ref);
    }

    let strings = array
        .iter()
        .filter_map(|s| s)
        .filter(|s| s.len() > 8)
        .map(|s| s.as_bytes())
        .collect::<Vec<_>>();

    let uncompressed_size = array
        .iter()
        .map(|s| s.map(|s| s.len() as u64).unwrap_or(0))
        .sum::<u64>();

    if strings.len() < num_samples {
        // There are fewer than NUM_SAMPLES strings that are large enough to justify compression.
        return Ok(StringCompressionByLeafResult {
            uncompressed_size,
            leaf_dict_size: 0,
            lz4_leaf_data_size: uncompressed_size,
            zstd_leaf_data_size: uncompressed_size,
        });
    }

    let samples = &strings[0..num_samples];

    let dict = zstd::dict::from_samples(samples, dict_size)
        .context("dict from continuous")
        .unwrap();

    let mut buffer = vec![];

    let mut zstd_leaf_data_size: u64 = 0;
    let mut lz4_leaf_data_size: u64 = 0;

    let zstd_dict = zstd::dict::EncoderDictionary::new(&dict, 0);

    for s in &strings {
        buffer.clear();
        let mut encoder = zstd::Encoder::with_prepared_dictionary(&mut buffer, &zstd_dict)
            .context("zstd encoder with prepared dict")?;
        encoder.write(s).context("zstd encoder write")?;
        let buffer = encoder.finish().context("zstd encoder finish")?;
        zstd_leaf_data_size += buffer.len() as u64;
    }

    for s in &strings {
        buffer.clear();
        let mut encoder =
            lzzzz::lz4::Compressor::with_dict(&dict).context("lz4 compressor create")?;
        let size = encoder
            .next_to_vec(s, &mut buffer, lzzzz::lz4::ACC_LEVEL_DEFAULT)
            .context("lz4 compressor next to vec")?;
        lz4_leaf_data_size += size as u64;
    }

    Ok(StringCompressionByLeafResult {
        uncompressed_size,
        leaf_dict_size: dict.len() as _,
        lz4_leaf_data_size,
        zstd_leaf_data_size,
    })
}

#[derive(Default)]
struct StringCompressionByLeafResult {
    uncompressed_size: u64,

    // Note: LZ4 and zstd use the same dictionary!
    leaf_dict_size: u64,

    lz4_leaf_data_size: u64,
    zstd_leaf_data_size: u64,
}

impl StringCompressionByLeafResult {
    pub fn combine(mut self, other: &mut Self) -> Self {
        let Self {
            uncompressed_size,
            leaf_dict_size,
            lz4_leaf_data_size,
            zstd_leaf_data_size,
        } = &mut self;

        *uncompressed_size += other.uncompressed_size;
        *leaf_dict_size += other.leaf_dict_size;
        *lz4_leaf_data_size += other.lz4_leaf_data_size;
        *zstd_leaf_data_size += other.zstd_leaf_data_size;
        self
    }
}

impl Display for StringCompressionByLeafResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            uncompressed_size,
            leaf_dict_size,
            lz4_leaf_data_size,
            zstd_leaf_data_size,
        } = self;

        let lz4 = lz4_leaf_data_size + leaf_dict_size;
        let zstd = zstd_leaf_data_size + leaf_dict_size;

        let lz4_reduction = -(1.0 - (lz4 as f64 / *uncompressed_size as f64)) * 100.0;
        let zstd_reduction = -(1.0 - (zstd as f64 / *uncompressed_size as f64)) * 100.0;

        writeln!(f, "uncompressed string data: {uncompressed_size: >10}")?;
        writeln!(f, "dictionary size (sum):    {leaf_dict_size: >10}")?;
        writeln!(
            f,
            "LZ4 size (sum+dict):      {lz4: >10} ({:+.02}%)",
            lz4_reduction
        )?;
        writeln!(
            f,
            "zstd size (sum+dict):     {zstd: >10} ({:+.02}%)",
            zstd_reduction
        )?;

        Ok(())
    }
}
