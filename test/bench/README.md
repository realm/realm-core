# Core Benchmarking Infrastructure

A benchmark is a subclass of the `Benchmark` base class.

The meat of a `Benchmark` derivative is the `measure()` function. This is what
actually gets measured. `measure()` is declared pure virtual in `Benchmark`:
you must (eventually) be override this method.

The `Benchmark` base class also has a couple other virtual functions, which
do nothing by default:

* `before_all()`
* `before_each()`
* `after_each()`
* `after_all()`

These functions (just like `measure()`), take no parameters. If you need
parameters, you should pass these using the instance variables of your
`Benchmark` derivative.

## Realm Benchmark

To make life easier, there is currently a one-size-fits-all bin for common
benchmark derivatives of `Benchmark` for testing Realm in
`util/realmbm.[ch]pp`. For instance,

* `WithSharedGroup` — Has a SharedGroup,
* `WithOneColumn` — Has a column of the given type.
* `WithEmptyRows` — Has a column filled with `N` empty rows.
* `WithRandomTs` — Has `N` random (numeric) `T`s,

These can be quite extensively parametrized by their template parameters.

## Getting Started

Include `util` in your project. Include `benchmark.hpp` and `realmbm.hpp`.

## FAQ

### How do I force exactly one repetition?

Override the `max_warmup_reps` and `max_reps` methods of the `Benchmark` base
class. For instance, to do one, measured repetition:

~~~~
// Subclass of Benchmark
    size_t max_reps()
    {
        return 1;
    }
    size_t max_warmup_reps()
    {
        return 0;
    }
~~~~
