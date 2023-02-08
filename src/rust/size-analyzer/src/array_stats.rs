use std::fmt::Display;
use std::io::Write;

use crate::bitscan::bit_width;

use rayon::prelude::IntoParallelIterator;
use rayon::prelude::ParallelIterator;
use realm_core::sys::NodeHeaderWidthType;
use realm_core::FrozenTransaction;

use super::UnsafeSendable;

use realm_core::sys::Array;

use realm_core::sys::ref_type;

use super::Analyzer;

pub struct ArrayStats;

impl Analyzer for ArrayStats {
    fn run(&mut self, txn: &FrozenTransaction) -> anyhow::Result<super::Analysis> {
        let top_ref = txn.top_ref();
        let mut compression_buffer = vec![];
        let result = unsafe { analyze_ref(&txn, top_ref, 0, &mut compression_buffer)? };
        Ok(Box::new(result))
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
    pub estimated_base_offset_size: u64,
    // pub estimated_base_offset_lz4_size: u64,
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
        self.estimated_base_offset_size += other.estimated_base_offset_size;
        // self.estimated_base_offset_lz4_size += other.estimated_base_offset_lz4_size;
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
            estimated_base_offset_size,
            // estimated_base_offset_lz4_size,
        } = self;

        let average_array_length = *array_length_sum as f64 / *num_arrays as f64;

        writeln!(f, "Recursion depth:  {recursion_depth: >10}")?;
        writeln!(f, "Reachable bytes:  {live_byte_size: >10}")?;
        writeln!(f, "Base+offset size: {estimated_base_offset_size: >10}")?;
        // writeln!(f, "Base+offset+LZ4:  {estimated_base_offset_lz4_size: >10}")?;
        writeln!(f, "Num arrays:       {num_arrays: >10}")?;
        writeln!(f, "Avg array length: {average_array_length: >10.2}")?;
        writeln!(f, "Arrays with refs: {arrays_with_refs: >10}")?;
        writeln!(f, "Arrays (bpnodes): {arrays_bpnode: >10}")?;
        writeln!(f, "{widths}")?;

        Ok(())
    }
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

/// Recursive analysis of an array ref and its children.
///
/// SAFETY: The ref must be valid for the given allocator.
pub(crate) unsafe fn analyze_ref(
    txn: &FrozenTransaction,
    ref_: ref_type,
    recursion_depth: usize,
    compression_buffer: &mut Vec<u8>,
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

    result.estimated_base_offset_size +=
        estimate_array_size_with_base_offset_encoding(&*array, None) as u64;

    // This takes a lot of time and doesn't give promising results.
    //
    // result.estimated_base_offset_lz4_size += estimate_array_size_with_base_offset_encoding(
    //     &*array,
    //     Some(BlobCompression::Lz4(compression_buffer)),
    // ) as u64;

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
                        let mut compression_buffer = vec![];
                        Some(analyze_ref(
                            txn,
                            ref_,
                            recursion_depth + 1,
                            &mut compression_buffer,
                        ))
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
                let child_result = analyze_ref(txn, ref_, recursion_depth + 1, compression_buffer)?;
                result = result.combine(child_result);
            }
        }
    }

    Ok(result)
}

pub(crate) fn is_ref(value: i64) -> bool {
    value != 0 && value & 1 == 0
}

#[derive(Clone, Copy)]
pub struct ArrayIter<'a, 'alloc> {
    pub(crate) array: &'a Array<'alloc>,
    pub(crate) i: usize,
    pub(crate) len: usize,
}

impl<'a, 'alloc> ArrayIter<'a, 'alloc> {
    pub fn new(array: &'a Array<'alloc>) -> Self {
        ArrayIter {
            array,
            i: 0,
            len: array.size(),
        }
    }
}

impl<'a, 'alloc> Iterator for ArrayIter<'a, 'alloc> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.len {
            let i = self.i;
            self.i += 1;
            unsafe { Some(self.array.get(i)) }
        } else {
            None
        }
    }
}

pub(crate) enum BlobCompression<'a> {
    Lz4(&'a mut Vec<u8>),
}

pub(crate) fn estimate_array_size_with_base_offset_encoding(
    array: &Array,
    blob_compression: Option<BlobCompression>,
) -> usize {
    const HEADER_SIZE: usize = 8;

    let wtype = array.width_type();
    let required_bytes = if wtype == NodeHeaderWidthType::wtype_Multiply {
        // Array is a list of chunks of `width` bytes.
        //
        // TODO: Consider if it makes sense to compress with a shared dictionary
        // (zstd), and then determine if we can use a smaller block size.
        array.get_width() * array.size() + HEADER_SIZE
    } else if wtype == NodeHeaderWidthType::wtype_Ignore {
        // Array is a chunk of bytes.

        let data_size = match blob_compression {
            Some(BlobCompression::Lz4(buffer)) => {
                // Estimate the size using LZ4 compression.
                buffer.clear();
                let mut encoder = lz4::EncoderBuilder::new()
                    .checksum(lz4::ContentChecksum::NoChecksum)
                    .build(buffer)
                    .unwrap();
                encoder.write(array.data()).unwrap();
                let buffer = encoder.finish().0;
                buffer.len()
            }
            None => array.size(),
        };

        data_size + HEADER_SIZE
    } else {
        // Array uses integer bit-encoding; determine if we can do better with
        // base+offset.

        let iter = ArrayIter::new(array);

        // Find the minimum value of the array. This allows for best encoding,
        // because negative integers always require at least 8 bits in Realm's
        // encoding, where positive integers can go as low as 1 bit.
        let Some(min_value) = iter.clone().min() else {
            return HEADER_SIZE;
        };

        let required_max_bitwidth = iter
            .map(|value| bit_width(value.wrapping_sub(min_value)))
            .max()
            .unwrap();

        let required_bits = HEADER_SIZE * 8 // Assuming that the header's size doesn't change.
        + bit_width(min_value).max(8) // Assuming at least 8 bits for the base.
        + required_max_bitwidth * array.size();

        // Round up to nearest byte
        (required_bits + 7) / 8
    };

    // Align to 8 bytes
    (required_bytes + 7) & !0x7
}
