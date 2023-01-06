/**
 * Experimental Realm query engine.
 *
 * TODO:
 *
 * - [ ] Use actual Realm tables as the data source.
 * - [ ] Adjust query state structure to better support parallelisation.
 * - [ ] Add custom aggregation. Note: Also needs the ability to coalesce
 *       the results from multiple runner threads.
 */

pub trait LeafScanner<'a, Src: LeafSource> {
    /// Reset the scanner and bind it to a data source (like a leaf).
    fn bind(&mut self, data: Option<&Src::Rows<'a>>);

    /// Find the next index where the condition is true.
    fn next_true(&mut self) -> Option<usize>;

    /// Find the next index where the condition is false.
    fn next_false(&mut self) -> Option<usize>;

    /// Evaluate the condition for an index in the current row source.
    fn is_true(&self, index: usize) -> bool;
}

impl<'a, Src> LeafScanner<'a, Src> for Box<dyn LeafScanner<'a, Src> + 'a>
where
    Src: LeafSource,
{
    fn bind(&mut self, data: Option<&Src::Rows<'a>>) {
        (**self).bind(data);
    }

    fn next_true(&mut self) -> Option<usize> {
        (**self).next_true()
    }

    fn next_false(&mut self) -> Option<usize> {
        (**self).next_false()
    }

    fn is_true(&self, index: usize) -> bool {
        (**self).is_true(index)
    }
}

/// Something that can give out handles to leaves, like a cluster tree.
pub trait LeafSource {
    type Rows<'a>: RowSource<'a>
    where
        Self: 'a;

    /// Number of leaves.
    fn len(&self) -> usize;

    fn get_leaf<'a>(&'a self, index: usize) -> Option<Self::Rows<'a>>;
}

/// Rows in a leaf.
pub trait RowSource<'a> {
    type Column<T>: ColumnSource<T>
    where
        T: Clone + Send + Sync + 'static;

    fn len(&self) -> usize;
    fn get_column<T: Clone + Send + Sync + 'static>(&self, index: usize) -> Self::Column<T>;
}

/// Array of values in a per-leaf column in a cluster tree.
pub trait ColumnSource<T> {
    fn get(&self, index: usize) -> Option<T>;
}

pub struct AndConditions<T> {
    //  AND conditions node.
    //
    // TODO: Sorted by cost - the cheapest comparison is first.
    conditions: Vec<T>,
    len: usize,
    i: usize,
}

impl<T> FromIterator<T> for AndConditions<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        AndConditions {
            conditions: Vec::from_iter(iter),
            len: 0,
            i: 0,
        }
    }
}

impl<'a, T, D> LeafScanner<'a, D> for AndConditions<T>
where
    T: LeafScanner<'a, D>,
    D: LeafSource,
{
    fn bind(&mut self, data: Option<&D::Rows<'a>>) {
        for cond in self.conditions.iter_mut() {
            cond.bind(data);
        }

        if let Some(data) = data {
            self.len = data.len();
        } else {
            self.len = 0;
        }
        self.i = 0;
    }

    fn next_true(&mut self) -> Option<usize> {
        // TODO: Zig-zag
        while self.i < self.len {
            let i = self.i;
            self.i += 1;
            if self.is_true(i) {
                return Some(i);
            }
        }

        None
    }

    fn next_false(&mut self) -> Option<usize> {
        // TODO: Zig-zag
        while self.i < self.len {
            let i = self.i;
            self.i += 1;
            if !self.is_true(i) {
                return Some(i);
            }
        }

        None
    }

    fn is_true(&self, index: usize) -> bool {
        for cond in self.conditions.iter() {
            if !cond.is_true(index) {
                return false;
            }
        }

        true
    }
}

pub struct OrConditions<T> {
    conditions: Vec<T>,
    len: usize,
    i: usize,
}

impl<T> FromIterator<T> for OrConditions<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        OrConditions {
            conditions: Vec::from_iter(iter),
            len: 0,
            i: 0,
        }
    }
}

impl<'a, T, D> LeafScanner<'a, D> for OrConditions<T>
where
    T: LeafScanner<'a, D>,
    D: LeafSource,
{
    fn bind(&mut self, data: Option<&D::Rows<'a>>) {
        for cond in self.conditions.iter_mut() {
            cond.bind(data);
        }

        if let Some(data) = data {
            self.len = data.len();
        } else {
            self.len = 0;
        }
        self.i = 0;
    }

    fn next_true(&mut self) -> Option<usize> {
        while self.i < self.len {
            let i = self.i;
            self.i += 1;
            if self.is_true(i) {
                return Some(i);
            }
        }

        None
    }

    fn next_false(&mut self) -> Option<usize> {
        while self.i < self.len {
            let i = self.i;
            self.i += 1;
            if !self.is_true(i) {
                return Some(i);
            }
        }

        None
    }

    fn is_true(&self, index: usize) -> bool {
        if self.conditions.len() == 0 {
            return true;
        }

        for cond in self.conditions.iter() {
            if cond.is_true(index) {
                return true;
            }
        }

        false
    }
}

pub struct NotCondition<T> {
    cond: T,
}

impl<'a, T, D> LeafScanner<'a, D> for NotCondition<T>
where
    T: LeafScanner<'a, D>,
    D: LeafSource,
{
    fn bind(&mut self, data: Option<&D::Rows<'a>>) {
        self.cond.bind(data);
    }

    fn next_true(&mut self) -> Option<usize> {
        self.cond.next_false()
    }

    fn next_false(&mut self) -> Option<usize> {
        self.cond.next_true()
    }

    fn is_true(&self, index: usize) -> bool {
        !self.cond.is_true(index)
    }
}

pub trait Comparison: Copy + std::fmt::Debug {
    const IDENTITY_IS_TRUE: bool;

    fn is_true<T: PartialEq + PartialOrd>(&self, a: &T, b: &T) -> bool;
}

#[derive(Clone, Copy, Debug)]
struct CmpEq;
#[derive(Clone, Copy, Debug)]
struct CmpNeq;
#[derive(Clone, Copy, Debug)]
struct CmpLt;
#[derive(Clone, Copy, Debug)]
struct CmpLte;
#[derive(Clone, Copy, Debug)]
struct CmpGt;
#[derive(Clone, Copy, Debug)]
struct CmpGte;

impl Comparison for CmpEq {
    const IDENTITY_IS_TRUE: bool = true;

    fn is_true<T: PartialEq + PartialOrd>(&self, a: &T, b: &T) -> bool {
        *a == *b
    }
}

impl Comparison for CmpNeq {
    const IDENTITY_IS_TRUE: bool = false;

    fn is_true<T: PartialEq + PartialOrd>(&self, a: &T, b: &T) -> bool {
        *a != *b
    }
}

impl Comparison for CmpLt {
    const IDENTITY_IS_TRUE: bool = false;

    fn is_true<T: PartialEq + PartialOrd>(&self, a: &T, b: &T) -> bool {
        *a < *b
    }
}

impl Comparison for CmpLte {
    const IDENTITY_IS_TRUE: bool = true;

    fn is_true<T: PartialEq + PartialOrd>(&self, a: &T, b: &T) -> bool {
        *a <= *b
    }
}

impl Comparison for CmpGt {
    const IDENTITY_IS_TRUE: bool = false;

    fn is_true<T: PartialEq + PartialOrd>(&self, a: &T, b: &T) -> bool {
        *a > *b
    }
}

impl Comparison for CmpGte {
    const IDENTITY_IS_TRUE: bool = true;

    fn is_true<T: PartialEq + PartialOrd>(&self, a: &T, b: &T) -> bool {
        *a >= *b
    }
}

pub struct CmpConstCond<'a, Src: LeafSource + 'a, T: Clone + Send + Sync + 'static, Cmp> {
    col_index: usize,
    col: Option<<Src::Rows<'a> as RowSource<'a>>::Column<T>>,
    needle: T,
    cmp: Cmp,
}

impl<'a, Src, T, Cmp> LeafScanner<'a, Src> for CmpConstCond<'a, Src, T, Cmp>
where
    Src: LeafSource,
    T: PartialEq + PartialOrd + Clone + Send + Sync + 'static,
    Cmp: Comparison + 'static,
{
    fn bind(&mut self, data: Option<&Src::Rows<'a>>) {
        if let Some(data) = data {
            self.col = Some(data.get_column(self.col_index));
        } else {
            self.col = None;
        }
    }

    fn next_true(&mut self) -> Option<usize> {
        todo!()
    }

    fn next_false(&mut self) -> Option<usize> {
        todo!()
    }

    fn is_true(&self, index: usize) -> bool {
        if let Some(ref col) = self.col {
            if let Some(value) = col.get(index) {
                return self.cmp.is_true(&value, &self.needle);
            }
        }

        false
    }
}

pub struct CmpColCond<'a, Src: LeafSource + 'a, T: Clone + Send + Sync + 'static, Cmp> {
    col_index_a: usize,
    col_index_b: usize,
    cols: Option<(
        <Src::Rows<'a> as RowSource<'a>>::Column<T>,
        <Src::Rows<'a> as RowSource<'a>>::Column<T>,
    )>,
    cmp: Cmp,
}

impl<'a, Src, T, Cmp> LeafScanner<'a, Src> for CmpColCond<'a, Src, T, Cmp>
where
    Src: LeafSource,
    T: PartialEq + PartialOrd + Clone + Send + Sync + 'static,
    Cmp: Comparison + 'static,
{
    fn bind(&mut self, data: Option<&Src::Rows<'a>>) {
        if let Some(data) = data {
            self.cols = Some((
                data.get_column(self.col_index_a),
                data.get_column(self.col_index_b),
            ));
        } else {
            self.cols = None;
        }
    }

    fn next_true(&mut self) -> Option<usize> {
        todo!()
    }

    fn next_false(&mut self) -> Option<usize> {
        todo!()
    }

    fn is_true(&self, index: usize) -> bool {
        if Cmp::IDENTITY_IS_TRUE && self.col_index_a == self.col_index_b {
            return true;
        }

        if let Some((ref col_a, ref col_b)) = self.cols {
            let Some(a) = col_a.get(index) else { return false; };
            let Some(b) = col_b.get(index) else { return false; };
            self.cmp.is_true(&a, &b)
        } else {
            false
        }
    }
}

/// Query state. This contains per-node mutable state that is modified as the
/// query runs. All column keys are resolved to indices in this representation.
pub struct QueryState<'a, LeafSrc: LeafSource + 'a> {
    conditions: Box<dyn LeafScanner<'a, LeafSrc> + 'a>,
    num_leaves: usize,
    leaf_i: usize,
    current_leaf_size: usize,
    index_offset: usize,
}

impl<'a, LeafSrc: LeafSource> QueryState<'a, LeafSrc> {
    pub fn new(mut scanner: Box<dyn LeafScanner<'a, LeafSrc> + 'a>, source: &'a LeafSrc) -> Self {
        let leaf = source.get_leaf(0);
        scanner.bind(leaf.as_ref());

        Self {
            num_leaves: source.len(),
            leaf_i: 0,
            index_offset: 0,
            current_leaf_size: if let Some(ref leaf) = leaf {
                leaf.len()
            } else {
                0
            },
            conditions: scanner,
        }
    }

    pub fn reset(&mut self, src: Option<&'a LeafSrc>) {
        if let Some(src) = src {
            self.num_leaves = src.len();
            self.leaf_i = 0;
            self.index_offset = 0;
            let leaf = src.get_leaf(0);
            self.current_leaf_size = if let Some(ref leaf) = leaf {
                leaf.len()
            } else {
                0
            };
            self.conditions.bind(leaf.as_ref());
        } else {
            self.num_leaves = 0;
            self.leaf_i = 0;
            self.index_offset = 0;
            self.current_leaf_size = 0;
            self.conditions.bind(None);
        }
    }
}

pub struct QueryRunner<'state, 'data, LeafSrc: LeafSource> {
    state: &'state mut QueryState<'data, LeafSrc>,
    data: &'data LeafSrc,
}

impl<'a, 'b, LeafSrc: LeafSource> Iterator for QueryRunner<'a, 'b, LeafSrc> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.state.leaf_i < self.state.num_leaves {
            if let Some(found) = self.state.conditions.next_true() {
                return Some(found + self.state.index_offset);
            } else {
                // Go to the next leaf.
                self.state.index_offset += self.state.current_leaf_size;
                self.state.leaf_i += 1;
                let next_leaf = self.data.get_leaf(self.state.leaf_i);
                if let Some(next_leaf) = next_leaf {
                    self.state.current_leaf_size = next_leaf.len();
                    self.state.conditions.bind(Some(&next_leaf));
                } else {
                    self.state.current_leaf_size = 0;
                    self.state.conditions.bind(None);
                }
            }
        }

        None
    }
}

impl<'state, 'data, LeafSrc: LeafSource> QueryRunner<'state, 'data, LeafSrc> {
    pub fn new(state: &'state mut QueryState<'data, LeafSrc>, data: &'data LeafSrc) -> Self {
        Self { state, data }
    }
}

pub mod ast {
    use super::{LeafScanner, LeafSource};

    // TODO: Consider using typestate pattern to represent a transformation from
    // string names (columns, tables) to keys, as well as various kinds of type
    // checks.
    pub enum Ast {
        And(Vec<Ast>),
        Or(Vec<Ast>),
        Not(Box<Ast>),
        ColumnConstComparison {
            // TODO: This should be column key, or even name.
            column_index: usize,
            value: Constant,
            comparison: Cmp,
        },
    }

    pub enum Cmp {
        Eq,
        Neq,
        Lt,
        Lte,
        Gt,
        Gte,
    }

    pub enum Constant {
        Integer(i64),
        Float(f64),
        Boolean(bool),
    }

    impl Ast {
        pub fn compile<'a, L: LeafSource + 'a>(&self) -> Box<dyn LeafScanner<'a, L> + 'a> {
            macro_rules! match_cmp_op {
                ($cmp:expr => |$cmp_op:ident| $($then:tt)*) => {
                    match $cmp {
                        Cmp::Eq => {
                            let $cmp_op = super::CmpEq;
                            $($then)*
                        }
                        Cmp::Neq => {
                            let $cmp_op = super::CmpNeq;
                            $($then)*
                        }
                        Cmp::Lt => {
                            let $cmp_op = super::CmpLt;
                            $($then)*
                        }
                        Cmp::Lte => {
                            let $cmp_op = super::CmpLte;
                            $($then)*
                        }
                        Cmp::Gt => {
                            let $cmp_op = super::CmpGt;
                            $($then)*
                        }
                        Cmp::Gte => {
                            let $cmp_op = super::CmpGte;
                            $($then)*
                        }
                    }
                };
            }

            macro_rules! match_value {
                ($value:expr => |$name:ident| $($then:tt)*) => {
                    match $value {
                        Constant::Integer($name) => { $($then)* },
                        Constant::Float($name) => { $($then)* },
                        Constant::Boolean($name) => { $($then)* },
                    }
                };
            }

            match self {
                Ast::And(ref terms) => Box::new(super::AndConditions::from_iter(
                    terms.iter().map(Ast::compile),
                )),
                Ast::Or(ref terms) => Box::new(super::OrConditions::from_iter(
                    terms.iter().map(Ast::compile),
                )),
                Ast::Not(ref term) => Box::new(super::NotCondition {
                    cond: term.compile(),
                }),
                Ast::ColumnConstComparison {
                    column_index,
                    value,
                    comparison,
                } => match_cmp_op! {
                    comparison => |cmp| {
                        match_value! { value => |value| {
                            Box::new(super::CmpConstCond {
                                col_index: *column_index,
                                col: None,
                                needle: *value,
                                cmp,
                            })
                        }}
                    }
                },
            }
        }
    }

    /// Unbound query
    pub struct Query {
        pub root: Ast,
    }

    impl Query {
        pub fn compile<'a, L: LeafSource>(&self, source: &'a L) -> super::QueryState<'a, L> {
            let scanner = self.root.compile::<L>();
            super::QueryState::new(scanner, source)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;

    // Const-generic `NUM_COLUMNS` (rather than using a `Vec` in `MockLeaf`) as
    // a crude way to ensure that all leaves have the same number of columns.
    struct MockTable<const NUM_COLUMNS: usize> {
        leaves: Vec<MockLeaf<NUM_COLUMNS>>,
    }

    struct MockLeaf<const NUM_COLUMNS: usize> {
        columns: [Box<dyn MockColumnTrait>; NUM_COLUMNS],
    }

    struct MockColumn<T> {
        values: Vec<T>,
    }

    // Quick dynamic indirection for column types. The alternative would be to
    // use a strongly-typed tuple, but the ergonomics of that are rather
    // terrible.
    trait MockColumnTrait: Any + Send + Sync {
        fn len(&self) -> usize;
        fn as_any(&self) -> &dyn std::any::Any;
    }

    impl<T: Clone + Send + Sync + 'static> MockColumnTrait for MockColumn<T> {
        fn len(&self) -> usize {
            self.values.len()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    impl<const N: usize> LeafSource for MockTable<N> {
        type Rows<'a> = &'a MockLeaf<N>;

        fn len(&self) -> usize {
            self.leaves.len()
        }

        fn get_leaf<'a>(&'a self, index: usize) -> Option<Self::Rows<'a>> {
            self.leaves.get(index)
        }
    }

    impl<'a, const N: usize> RowSource<'a> for &'a MockLeaf<N> {
        type Column<T> = &'a MockColumn<T> where T: Clone + Send + Sync + 'static;

        fn len(&self) -> usize {
            self.columns[0].len()
        }

        fn get_column<T: Clone + Send + Sync + 'static>(&self, index: usize) -> Self::Column<T> {
            let column = self.columns.get(index).expect("invalid column index");
            column.as_any().downcast_ref().expect("invalid column type")
        }
    }

    impl<'a, T: Clone + 'static> ColumnSource<T> for &'a MockColumn<T> {
        fn get(&self, index: usize) -> Option<T> {
            self.values.get(index).cloned()
        }
    }

    #[test]
    fn mock_query() {
        let table = MockTable {
            leaves: vec![
                MockLeaf {
                    columns: [
                        Box::new(MockColumn::<i64> {
                            values: vec![1, 2, 3],
                        }),
                        Box::new(MockColumn::<f64> {
                            values: vec![1.0, 2.0, 3.0],
                        }),
                        Box::new(MockColumn::<String> {
                            values: vec![String::from("a"), String::from("b"), String::from("c")],
                        }),
                    ],
                },
                MockLeaf {
                    columns: [
                        Box::new(MockColumn::<i64> {
                            values: vec![4, 5, 6],
                        }),
                        Box::new(MockColumn::<f64> {
                            values: vec![4.0, 5.0, 6.0],
                        }),
                        Box::new(MockColumn::<String> {
                            values: vec![String::from("d"), String::from("e"), String::from("f")],
                        }),
                    ],
                },
            ],
        };

        use ast::{Ast, Constant};
        let query_ast = ast::Query {
            root: Ast::And(vec![
                Ast::ColumnConstComparison {
                    column_index: 0,
                    value: Constant::Integer(4),
                    comparison: ast::Cmp::Lte,
                },
                Ast::ColumnConstComparison {
                    column_index: 1,
                    value: Constant::Float(1.0),
                    comparison: ast::Cmp::Gt,
                },
            ]),
        };

        let mut query_state = query_ast.compile(&table);
        let runner = QueryRunner::new(&mut query_state, &table);

        let matches = runner.collect::<Vec<_>>();
        assert_eq!(matches, [1, 2, 3]);
    }
}
