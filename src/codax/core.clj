(ns codax.core
  (:require
   [codax.operations :as ops]
   [codax.prefix :refer [set-prefix prefix-path]]
   [codax.store :as store])
  (:gen-class))

(defn open-database
  "Opens a database at the given filepath. If a database is already open at the given path
  an Exception is thrown.

  By default a set of files in the database directory with the suffix ARCHIVE are created or
  overwritten on each compaction and represent the most recent pre-compaction state.

  Alteratively, timestamped archives will be created if a `:backup-fn` argument is supplied.
  Once the timestamped archive-files are created, the `:backup-fn` function will be called
  (in a future) with a map containing the following keys:

  :dir ; a string representing the full path to the database directory
  :suffix ; a string of the timestamp suffix attached to the files
  :file-names ; a vector of strings representing the generated file names

  (see codax.backup/make-backup-archiver)
  "
  [filepath & {:keys [backup-fn]}]
  (store/open-database filepath backup-fn))

(defn close-database
  "Will close the database at the provided filepath (or the filepath of the a database map)
  Attempts to use a closed or invalidated database will throw an Exception."
  [filepath-or-db]
  (store/close-database filepath-or-db))

(defn get-at
  "Returns the map or othre value at the supplied `path`. If it has been modified within
  the current transaction, it will evaluate to the modified value

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  ([tx]
   (ops/collect tx []))
  ([tx path]
   (ops/collect tx (prefix-path tx path))))

(defn assoc-at
  "Associates a path with a value or object, overwriting the current value (and all nested
  values)

  The values will be changed in the datastore when the transaction is committed
  (occurs  after the `body` of a `with-write-transaction` form).

  All paths will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path val-or-map]
  (ops/assoc-path tx (prefix-path tx path) val-or-map))

(defn update-at
  "Runs a function on the current map or value at the supplied path and `assoc-at`s the result.

  The values will be changed in the datastore when the transaction is committed
  (occurs  after the `body` of a `with-write-transaction` form).

  All paths will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path f & args]
  (apply ops/update-path tx (prefix-path tx path) f args))

(defn merge-at
  [tx path m]
  (apply ops/update-path tx (prefix-path tx path) merge m))


(defn dissoc-at
  "Deletes all values at the supplied path.

  The value will be deleted from the datastore when the transaction is committed, (which
  (occurs  after the `body` of a `with-write-transaction` form).

  The path will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path]
  (ops/delete-path tx (prefix-path tx path)))

;;;; Transactions

(defmacro with-write-transaction
  "Creates a write transaction object and assigns it to the `tx-symbol`.
  This transaction object can be passed to any of the transaction functions.
  The `body` must evaluate to this object so it can be committed."
  [[database tx-symbol & {:keys [prefix]}] & body]
  `(store/with-write-transaction [~database ~tx-symbol]
     (let [~tx-symbol (set-prefix ~tx-symbol ~prefix)]
       ~@body)))

(defmacro with-read-transaction
  "Creates a read transaction object and assigns it to `tx-symbol`.
  This transaction object can be passed to the non-modifying transaction functions.
  Behavior when passed to a modifying transaction function is undefined.
  The final value will be the result of evaluating the `body`."
  [[database tx-symbol & {:keys [prefix]}] & body]
  `(store/with-read-transaction [~database ~tx-symbol]
     (let [~tx-symbol (set-prefix ~tx-symbol ~prefix)]
       ~@body)))

;;;; Direct Database Convenience Functions

(defmacro with-result-transaction [[db-sym tx-sym path-sym] & body]
  `(let [res# (atom nil)]
     (with-write-transaction [~db-sym ~tx-sym]
       (let [tx-res# (do ~@body)]
         (reset! res# (get-at tx-res# ~path-sym))
         tx-res#))
     @res#))

(defn get-at!
  "Wraps a `get-at` call in a read transaction for convenience.
  Returns the result at the provided path.
  Warning: Will load and return the entire database if no path argument is supplied.
  See (doc codax.core/get-at)"
  ([db]
   (get-at! db []))
  ([db path]
   (with-read-transaction [db tx]
     (get-at tx path))))

(defn assoc-at!
  "Wraps an `assoc-at` call in a write transaction for convenience.
  Returns the result at the provided path.
  See (doc codax.core/assoc-at)"
 [db path val-or-map]
  (with-result-transaction [db tx path]
    (assoc-at tx path val-or-map)))

(defn update-at!
  "Wraps an `update-at` call in a write transaction for convenience.
  Returns the result at the provided path.
  See (doc codax.core/update-at)"
  [db path f & args]
  (with-result-transaction [db tx path]
    (apply update-at tx path f args)))

(defn merge-at!
  "Wraps a `merge-at` call in a write transaction for convenience.
  Returns the result at the provided path.
  See (doc codax.core/merge-at)"
  [db path m]
  (with-result-transaction [db tx path]
    (merge-at tx path m)))

(defn dissoc-at!
  "Wraps a `dissoc-at` call in a write transaction for convenience.
  Returns the result at the provided path.
  See (doc codax.core/dissoc-at)"
  [db path]
  (with-result-transaction [db tx path]
    (dissoc-at tx path)))

;;;; Main

(defn -main []
  (clojure.main/repl
   :init (fn []
           (in-ns 'codax.core)
           (require '[clojure.repl :refer :all]
                    '[clojure.pprint :refer [pprint]]
                    '[codax.bench.performance :refer [run-benchmark]]))))
