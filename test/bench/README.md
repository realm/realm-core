# Core Benchmarking Infrastructure

A benchmark is a subclass of the `Benchmark` base class.

The meat of a `Benchmark` base class is the `bench()` function. This is what
will actually get measured. `bench()` is declared pure virtual in `Benchmark`,
and so you must (eventually) be override it.

The `Benchmark` base class also has a couple other virtual functions, which
do nothing in the base class:

* `before_all()`
* `before_each()`
* `after_each()`
* `after_all()`

These functions (just like `bench()`), take no parameters. If you need
parameters, you should pass these using the instance variables of your
`Benchmark` derivative.

## Getting Started

Include `util` in your project. Include `benchmark.hpp`.

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
