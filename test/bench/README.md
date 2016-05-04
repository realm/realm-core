# Core Benchmarking Infrastructure

A benchmark is a subclass of the `Benchmark` base class.

## Getting Started

Include `util` in your project. Include `benchmark.hpp`.

## FAQ

### How do I force exactly one repetition?

Override the `max_reps` method of the `Benchmark` base class:
