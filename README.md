# codax

Codax is an idiomatic transactional embedded database for clojure. A codax database functions as a single (potentially enormous) clojure map, with a similar access & modification api.

[![Clojars Project](http://clojars.org/codax/latest-version.svg)](http://clojars.org/codax)

### The Why

Even simple programs frequently benefit from saving data to disk. Unfortunately, there is generally a large semantic leap once you go from values in memory to values on disk. Codax aims to close that semantic gap. While it won't win any speed contests, it is designed to be performant enough for applications serving thousands of users. Most importantly, it is designed to make data persistance as low friction as possible. It is also designed to be effortless to get started with. There are no external libraries to install, and the underlying B+ tree is written directly in clojure.

Fundamentally, I wrote this library for myself in an ongoing effort to make my own life simpler and my own programming more fun. I wanted to share it with the community in the hopes that others may find it does the same for them.


### ACID Compliance

Codax provides the following guarantees:

  - **Atomic** - Every transaction is completed fully, or not at all
  - **Consistent** - The database always represents a valid clojure map
  - **Isolated** - No reader or writer will ever see data from an incomplete transaction
  - **Durable** - All writes are synced to disk before returning

### Production Ready?

I have successfully used this library in production environments. That said, there are probably a few rough edges that could use smoothing.

## Usage

### Basic API

**Database Functions**

  - `open-database` - Opens or creates a database, or returns an existing database connection if it's already open
  - `close-database` - Closes an open database
  - `destroy-database!` - Deletes a database and all its data _irretrievably_ (intended for use in tests).

**Transaction Macros**

These take a database argument and a transaction-symbol and bind the symbol to a newly created transaction. Transactions are isolated from each other. Read-transactions evaluate to the value of their body, but a (successful) **write-transaction evaluates to nil**.

  - `with-read-transaction` - creates a read transaction
  - `with-write-transaction` - creates a write transaction (body must evaluate to a transaction or an exception will be thrown)

**In-Transaction Functions**

These are all similar to the clojure.core map `*-in` (e.g. `assoc-in`) with the following exceptions:

  - their first argument is a **transaction** instead of a map
  - their second argument is a **path** (see below)
  - their value argument or result (in the case of update) must be [conformant](#conformant-values)

These must be called within a `with-write-transaction` or a `with-read-transaction` expression. Changes will only be persistent if `with-write-transaction` is used.

  - `get-at`
  - `assoc-at`
  - `update-at`
  - `merge-at`
  - `dissoc-at`

**Shortcut Functions**

These are the same as the transactional-functions except that their first argument is a **database** instead of a **transaction**. These are convenience functions which automatically create and execute transactions. The write variants will also return the result of the modification.

  - `get-at!`
  - `assoc-at!`
  - `update-at!`
  - `merge-at!`
  - `dissoc-at!`

### Paths
A `path` is a vector of keys similar to the `[k & ks]` used in function like `assoc-in` with a few exceptions:

  - they are **limited to the following types**:
	- Symbols
	- Keywords
	- Strings
	- Numbers (float/double use is _strongly discouraged_)
	- true
	- false
	- nil
	- java.time.Instant
	- org.joda.time.DateTime
  - the path can only target nested maps, and **cannot be used to descend into other data structures (e.g. vectors)**.
  - you can get the empty path (e.g. `(get-at db [])` returns the full database) but you cannot modify it (e.g. `(assoc-at [] :foo)` throws an error)

If you are interested in contributing support for additional types, please review [doc/types.md](https://github.com/dscarpetti/codax/blob/master/doc/types.md).


### Conformant Values

  - non-map values of any type serializable by [nippy](https://github.com/ptaoussanis/nippy)
    - _this will only be relevant to you if you are storing custom records or exotic datatypes. Out of the box, virtually all standard clojure datatypes are supported (i.e. you don't need to do anything special to store lists/vectors/sets/etc.)_
    - _the serialization is performed automatically, you **do not** need to serialize values manually_
  - maps and nested maps whose **keys conform to the valid path types** listed above

### Transactions

**Immutability**

Transactions are immutable. Each transformation (e.g. `assoc-at`, `update-at`) returns a new transaction, it _does not modify_ the transaction. Essentially you should treat them as you would a standard clojure map, one that you interact with using the `*-at` functions.

_Example:_
```
(c/with-write-transaction [db tx-original]
  (let [tx-a (c/assoc-at tx-original [:letter] "a")
        tx-b (c/assoc-at tx-original [:letter] "b")]
	tx-a))

(c/get-at! db [:letter]) ; "a"
```

See the [FAQ](#frequently-asked-questions) for examples of potential pitfalls.

**Visibility**

Changes in a transaction are only visible to subsequent transformations on that transaction. They are not visible anywhere else until committed (by being the final result in the body of a `with-write-transaction` expression). The changes are also not visible in any read transaction opened before the write transaction is committed.

_Example:_
```clojure
(c/with-write-transaction [db tx]
  (-> tx
      (c/assoc-at [:number] 1000)
	  (c/update-at [:number] inc)))

(c/get-at! db [:number]) ; 1001

```

**Exceptions**

If an Exception is thrown within a `with-write-transaction` expression, the transaction is aborted and no changes are persisted.

**Locking**

Write transactions block other write transactions (though they do not block read transactions). It is best to avoid doing any computationally complex or IO heavy tasks (such as fetching remote data) inside a `with-write-transaction` block. See [Performance](#performance) for more details.


## Examples

``` clojure
(require [codax.core :as c])
```

### Simple Use
``` clojure

(def db (c/open-database "data/demo-database")) ;

(c/assoc-at! db [:assets :people] {0 {:name "Alice"
                                      :occupation "Programmer"
                                      :age 42}
                                   1 {:name "Bob"
                                      :occupation "Writer"
                                      :age 27}}) ; {0 {:age 42, :name "Alice", ...}, 1 {:age 27, :name "Bob", ...}}

(c/get-at! db [:assets :people 0]) ; {:name "Alice" :occupation "Programmer" :age 42}

(c/update-at! db [:assets :people 1 :age] inc) ; 28

(c/merge-at! db [:assets] {:tools {"hammer" true
                                   "keyboard" true}}) ; {:people {...} :tools {"hammer" true, "keyboard" true}}

(c/get-at! db [:assets])
;;  {:people {0 {:name "Alice"
;;               :occupation "Programmer"
;;               :age 42}
;;            1 {:name "Bob"
;;               :occupation "Writer"
;;               :age 28}}
;;   :tools {"hammer" true
;;           "keyboard" true}}

(c/close-database db)
```

### Transaction Example

``` clojure
(def db (c/open-database "data/demo-database"))

;;;; init
(c/with-write-transaction [db tx]
  (c/assoc-at tx [:counters] {:id 0 :users 0}))

;;;; user fns
(defn add-user
  "create a user and assign them an id"
  [username]
  (c/with-write-transaction [db tx]
	(when (c/get-at tx [:usernames username] )
	  (throw (Exception. "username already exists")))
	(let [user-id (c/get-at tx [:counters :id])
		  user {:id user-id
				:username username
				:timestamp (System/currentTimeMillis)}]
	  (-> tx
		  (c/assoc-at [:users user-id] user)
		  (c/assoc-at [:usernames username] user-id)
		  (c/update-at [:counters :id] inc)
		  (c/update-at [:counters :users] inc)))))

(defn get-user
  "fetch a user by their username"
  [username]
  (c/with-read-transaction [db tx]
	(when-let [user-id (c/get-at tx [:usernames username])]
	  (c/get-at tx [:users user-id]))))

(defn rename-user
  "change a username"
  [username new-username]
  (c/with-write-transaction [db tx]
	(when (c/get-at tx [:usernames new-username] )
	  (throw (Exception. "username already exists")))
	(when-let [user-id (c/get-at tx [:usernames username])]
	  (-> tx
		  (c/dissoc-at [:usernames username])
		  (c/assoc-at [:usernames new-username] user-id)
		  (c/assoc-at [:users user-id :username] new-username)))))

(defn remove-user
  "remove a user"
  [username]
  (c/with-write-transaction [db tx]
	(when-let [user-id (c/get-at tx [:usernames username])]
	  (-> tx
		  (c/dissoc-at [:username username])
		  (c/dissoc-at [:users user-id])
		  (c/update-at [:counters :users] dec)))))


;;;;; edit users

(c/get-at! db) ; {:counters {:id 0, :users 0}}


(add-user "charlie") ; nil
(c/get-at! db)
;;  {:counters {:id 1, :users 1},
;;   :usernames {"charlie" 0},
;;   :users {0 {:id 0, :timestamp 1484529469567, :username "charlie"}}}


(add-user "diane") ; nil
(c/get-at! db)
;;  {:counters {:id 2, :users 2},
;;   :usernames {"charlie" 0, "diane" 1},
;;   :users
;;   {0 {:id 0, :timestamp 1484529603440, :username "charlie"},
;;    1 {:id 1, :timestamp 1484529603444, :username "diane"}}}


(rename-user "charlie" "chuck") ; nil
(c/get-at! db)
;;  {:counters {:id 2, :users 2},
;;   :usernames {"chuck" 0, "diane" 1},
;;   :users
;;   {0 {:id 0, :timestamp 1484529702868, :username "chuck"},
;;    1 {:id 1, :timestamp 1484529702872, :username "diane"}}}


(remove-user "diane") ; nil
(c/get-at! db)
;;  {:counters {:id 2, :users 1},
;;   :usernames {"chuck" 0, "diane" 1},
;;   :users {0 {:id 0, :timestamp 1484529782527, :username "chuck"}}}



(c/close-database db)

```

## Frequently Asked Questions

**Why aren't all my changes being saved?**

Because transactions are immutable, if an updated transaction is discarded, the transformations it contains will not be committed.

_Incorrect:_
```clojure
(c/with-write-transaction [db tx]
  (c/assoc-at tx [:users 1] "Alice") ; this write is "lost"
  (c/assoc-at tx [:users 2] "Bob"))

(c/get-at! db [:users]) ; {2 "Bob"}
```

_Correct:_
```clojure
(c/with-write-transaction [db tx]
  (-> tx ; thread the transaction through multiple transformations
      (c/assoc-at [:users 1] "Alice")
      (c/assoc-at [:users 2] "Bob")))

(c/get-at! db [:users]) ; {1 "Alice" 2 "Bob"}
```

**Why am I getting a NullPointerException in my Write Transaction?**

A common cause is that the body of the `with-write-transaction` form is not evaluating to (returning) a transaction.

_Incorrect:_
```clojure
(defn init-counter! []
  (c/with-write-transaction [db tx]
    (when-not (c/get-at tx [:counter])
      (c/assoc-at tx [:counter] 0))))

(init-counter!) ; nil (it works the first time)
(init-counter!) ; java.lang.NullPointerException (the body evaluates to nil)

```

_Correct:_
```clojure
(defn init-counter! []
  (c/with-write-transaction [db tx]
    (if-not (c/get-at tx [:counter])
      (c/assoc-at tx [:counter] 0)
      tx))) ;; if the counter is already initialized, return the unmodified transaction

(init-counter!) ; nil
(init-counter!) ; nil

```

## Performance

_Codax is geared towards read-heavy workloads._

  - Read-Transactions block _nothing_
  - Write-Transactions block other Write-Transactions
  - Stage-1 Compaction blocks Write-Transactions (slow)
  - Stage-2 Compaction blocks both Reader-Transactions and Write-Transactions (fast)


### Benchmark Results

**Jan 14, 2017**

The following figures are for a database populated with 16,000,000 (map-leaf) values running on a Digital Ocean 2-core 2GB RAM instance. The write transactions have an average "path" length of 6 and an average 7 leaf values.

  - ~320 write-transaction/second
  - ~1640 read-transactions/second
  - ~2700ms per compaction (compaction happens automatically every 10,000 writes)

These values come from running the `codax.bench.performace/run-benchmark` benchmarking function without arguments 3 times consecutively.

## Testing

You can run the tests from the command line with `lein test`.

A few notes:

* Tests that are expected to throw errors will print those errors to the console, that does not indicate that the test has failed.
* The store-test may lag for a minute or two as it runs a test interleaving 20,000 writes with 100,000 reads.
* Any help expanding and normalizing the testing would be greatly appreciated.


### Bugs

...

## Contributing

Insights, suggestions, and PRs are very welcome.

## License

Copyright © 2017 David Scarpetti

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
