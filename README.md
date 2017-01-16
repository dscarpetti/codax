# codax

Codax is an idiomatic transactional disk-based datastore for clojure. A codax database functions as a single (potentially enormous) clojure map, with a similar access & modification api.

### ACID Compliance

Codax provides the following guarantees:

  - **Atomic** - Every transaction is completed fully, or not at all
  - **Consistent** - The database always represents a valid clojure map
  - **Isolated** - No reader or writer will ever see data from an incomplete transaction
  - **Durable** - All writes are synced to disk before returning

## Installation

```clojure
[codax "1.0.0-SNAPSHOT"]
```

## Usage

### Basic API

####Database Functions

  - `open-database` - Opens or creates a database
  - `close-database` - Closes an open database

####Transaction Macros
These take a database argument and a transaction-symbol and bind the symbol to a newly created transaction. Transactions are isolated from each other. Read-transactions evaluate to the value of their body, but a (successful) **write-transaction evaluates to nil**.

  - `with-read-transaction` - creates a read transaction
  - `with-write-transaction` - creates a write transaction (body must evaluate to a transaction or an exception will be thrown)

####In-Transaction Functions
These are all similar to the clojure.core map `*-in` (e.g. `assoc-in`) with the following exceptions:

  - their first argument is a **transaction** instead of a map
  - their second argument is a **path** (see below)
  - their value argument or result (in the case of update) must be **conformant** (see below)

These must be called within a `with-write-transaction` or a `with-read-transaction` expression. Changes will only persisent if `with-write-transaction` is used.

  - `get-at`
  - `assoc-at`
  - `update-at`
  - `merge-at`
  - `dissoc-at`

####Shortcut Functions
These are the same as the transactional-functions except that their first argument is a **database** instead of a **transaction**. These are convenience functions which automatically create and execute transactions.

  - `get-at!`
  - `assoc-at!`
  - `update-at!`
  - `merge-at!`
  - `dissoc-at!`

### Paths
A `path` is a vector of keys similar to the `[k & ks]` used in function like `assoc-in` with a few exceptions

  - they are **limited to the following types**:
	- Symbols
	- Keywords
	- Strings
	- Numbers (float/double use is _strongly discouraged_)
	- true
	- false
	- nil
	- org.joda.time.DateTime
  - the path can only target nested maps, and **cannot be used to descend into other data structures (e.g. arrays)**.
  - you can get the empty path (e.g. `(get-at db [])` returns the full database) but you cannot modify it (e.g. `(assoc-at [] :foo)` throws an error)

### Conformant Values

  - non-map values of any type serializable by [nippy](https://github.com/ptaoussanis/nippy)
  - maps and nested maps whose **keys conform to the valid path types** listed above.

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
									:age 27}}) ; nil

(c/get-at! db [:assets :people 0]) ; {:name "Alice" :occupation "Programmer" :age 42}

(c/update-at! db [:assets :people 1 :age] inc) ; nil

(c/merge-at! db [:assets] {:tools {"hammer" true
								 "keyboard" true}}) ; nil

(c/get-at! db [:assets])
;;  {:people {0 {:name "Alice"
;;               :occupation "Programmer"
;;               :age 42}
;;            1 {:name "Bob"
;;               :occupation "Writer"
;;               :age 27}}
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
  "remove a uset"
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

## Performance

**Codax is geared towards read-heavy workloads.**

  - Read-Transactions block _nothing_
  - Write-Transactions block other Write-Transactions
  - Stage-1 Compaction blocks Write-Transactions (slow)
  - Stage-2 Compaction blocks both Reader-Transactions and Write-Transactions (fast)


### Benchmark Results

####Jan 14, 1017

The following figures are for a database populated with 16,000,000 (map-leaf) values running on a Digital Ocean 2-core 2GB RAM instance. The write transactions have an average "path" length of 6 and an average 7 leaf values.

  - ~320 write-transaction/second
  - ~1640 read-transactions/second
  - ~2700ms per compaction (compaction happens automatically every 10,000 writes)

These values come from running the `codax.bench.performace/run-benchmark` benchmarking function without arguments 3 times consecutively.

## Options

...

### Bugs

...

## License

Copyright © 2016 David Scarpetti

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
