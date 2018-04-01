# Change Log

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
