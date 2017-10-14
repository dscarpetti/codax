# Change Log

## 1.1.0

* thread-safe opening/closing/destroying databases
* [TODO] make `destroy-database` function available in core namespace
* [TODO] `list-databases` function - returns a list of open databases
* [TODO] `close-all-databases` function - closes all open databases
* [TODO] `is-open?` predicate

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
