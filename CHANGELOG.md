# Change Log

## 1.5.0 (2024-02-15

* Allow unsorted maps and sets to be used as keys in paths

## 1.4.0 (2023-07-20)

* implement upgradable transactions (using `with-upgradable-transaction` macro)
* update taoensso/nippy to 3.2.0 to address RCE vulnerability: [taoensso/nippy#130](https://github.com/taoensso/nippy/issues/130)

### Upgrading From 1.3.1

The upgraded nippy requires non-standard types to be whitelisted before they can be decoded. When opening a database created with an earlier version of codax nippy may throw security errors when thawing non-standard values. This is easily corrected. See discussion in [#30](https://github.com/dscarpetti/codax/pull/30)

## 1.3.1 (2019-05-21)

* no longer AOT compiled

## 1.3.0 (2018-06-18)

* dynamic compaction scheduling
  * better support for smaller databases
  * closing/opening databases no longer delays compaction
* rewrite and simplify pathwise encoding engine for clarity and extensibility. Add:
  * add `defpathtype` - define custom path type encodings by specifying an encoder and decoder
  * add `check-path-encoding` - helper function for testing new path type encodings
  * add `path-encoding-assignments` - helper function to view and avoid redefining existing path types
  * specific documentation added to doc/types.md
* update `nippy` dependency to 2.14.0
* simplify and update benchmarking

## 1.2.1 (2018-06-13)

* fix manifest overgrowth bug
  * references to discarded nodes were not being removed from the manifest which caused the manifest to grow unboundedly. The issue was particularly pronounced in rewrite-heavy workloads.

## 1.2.0 (2018-01-22)

* seek-support - exposed seeking for records (forward and reverse) with documentation and examples
  * `seek-at` & `seek-at!`
  * `seek-from` & `seek-from!`
  * `seek-to` & `seek-to!`
  * `seek-range` & `seek-range!`
  * `seek-prefix` & `seek-prefix!`
  * `seek-prefix-range` & `seek-prefix-range!`
* better exceptions for common errors
  * clojure.lang.ExceptionInfo: Invalid Database
  * clojure.lang.ExceptionInfo: Invalid Transaction
* dropped `^:skip-aot` directive
* codax.core/-main is now private
* tests no longer log any output by default
* moved dev deps and utils to :dev profile

## 1.1.0 (2017-11-8)

* thread-safe opening/closing/destroying databases
* add `destroy-database!` to core namespace
* deprecate `open-database` and replace it with `open-database!`
* deprecate `close-database` and replace it with `close-database!`
* `close-all-databases!` function - closes all open databases
* `is-open?` database predicate

## 1.0.3 (2017-10-12)

* documentation improvements
  * FAQ for possible gotchas
* fix missing `clojure.main` require in core namespace

## 1.0.2 (2017-09-26)

* support for `java.time.Instant`s
* fix/remove erroneous `nil` value notes in core namespace docstrings

## 1.0.1 (2017-09-25)

* convenience methods (like `update-at!`) now return their results
* add tests for convenience methods
