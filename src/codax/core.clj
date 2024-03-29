(ns codax.core
  (:require
   [clojure.main]
   [codax.operations :as ops]
   [codax.pathwise :as pathwise]
   [codax.prefix :refer [set-prefix prefix-path]]
   [codax.store :as store]))

(defn is-open?
  "Takes a database or a path and returns true if the database is open.
  If the database is closed (or does not exist) false is returned."
  [filepath-or-db]
  (store/is-open? filepath-or-db))

(defn open-database!
  "Opens a database at the given filepath. If a database is already open at the given path
  the existing database connection is returned.

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

(defn close-database!
  "Safely closes the database at the provided filepath (or the filepath of the a database map)
  Attempts to use a closed or invalidated database will throw an Exception."
  [filepath-or-db]
  (store/close-database filepath-or-db))

(defn close-all-databases!
  "Safely closes all open databases"
  []
  (store/close-all-databases))

(defn destroy-database!
  "Removes database files and generic archive files.
  If there is nothing else in the database directory, it is also removed.
  If open, the database will be closed.

  This function is predominantly intended to facilitate testing."
  [filepath-or-db]
  (store/destroy-database filepath-or-db))

(defn open-database
  "DEPRECATED - use `open-database!` instead"
  {:deprecated "1.1.0"}
  [filepath & {:keys [backup-fn]}]
  (println "WARNING: codax.core/open-database is deprecated, use codax.core/open-database! instead")
  (open-database! filepath :backup-fn backup-fn))

(defn close-database
  "DEPRECATED - use `close-database!` instead"
  {:deprecated "1.1.0"}
  [filepath-or-db]
  (println "WARNING: codax.core/close-database is deprecated, use codax.core/close-database! instead")
  (close-database! filepath-or-db))

(defn get-at
  "Returns the map or other value at the supplied `path`. If it has been modified within
  the current transaction, it will evaluate to the modified value

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  ([tx]
   (store/assert-txn tx)
   (let [path (prefix-path tx [])
         tx (store/view-path! tx path)]
     (ops/collect tx path)))
  ([tx path]
   (store/assert-txn tx)
   (let [path (prefix-path tx path)
         tx (store/view-path! tx path)]
     (ops/collect tx path))))

(defn assoc-at
  "Associates a path with a value or object, overwriting the current value (and all nested
  values)

  The values will be changed in the datastore when the transaction is committed
  (occurs  after the `body` of a `with-write-transaction` form).

  All paths will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path val-or-map]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/touch-path! tx path)]
    (ops/assoc-path tx path val-or-map)))

(defn update-at
  "Runs a function on the current map or value at the supplied path and `assoc-at`s the result.

  The values will be changed in the datastore when the transaction is committed
  (occurs  after the `body` of a `with-write-transaction` form).

  All paths will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path f & args]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/touch-path! tx path)]
    (apply ops/update-path tx path f args)))

(defn merge-at
  [tx path m]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/touch-path! tx path)]
    (apply ops/update-path tx path merge m)))

(defn dissoc-at
  "Deletes all values at the supplied path.

  The value will be deleted from the datastore when the transaction is committed, (which
  (occurs  after the `body` of a `with-write-transaction` form).

  The path will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/touch-path! tx path)]
    (ops/delete-path tx path)))

;;;;

(defn seek-at
  "Provides key-value pairs ordered by key of the map at the provided `path`.

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path & {:keys [limit reverse]}]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/view-path! tx path)]
    (ops/seek-path tx path limit reverse)))

(defn seek-prefix
  "Provides key-value pairs ordered by key of the map at the provided `path`
  for all keys beginning with `val-prefix`.

  Note: `val-prefix` should be a string or keyword.

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path val-prefix & {:keys [limit reverse]}]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/view-path! tx path)]
    (ops/seek-prefix tx path val-prefix limit reverse)))

(defn seek-prefix-range
  "Provides key-value pairs ordered by key of the map at the provided `path`
  for all keys beginning with a prefix between `start-prefix` & `end-prefix`

  Note: `start-prefix` & `end-prefix` should be strings or keywords.

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path start-prefix end-prefix & {:keys [limit reverse]}]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/view-path! tx path)]
    (ops/seek-prefix-range tx path start-prefix end-prefix limit reverse)))

(defn seek-from
  "Provides key-value pairs ordered by key of the map at the provided `path`
  for all keys >= `start-val`.

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path start-val & {:keys [limit reverse]}]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/view-path! tx path)]
    (ops/seek-from tx path start-val limit reverse)))

(defn seek-to
  "Provides key-value pairs ordered by key of the map at the provided `path`
  for all keys <= `end-val`.

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path end-val & {:keys [limit reverse]}]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/view-path! tx path)]
    (ops/seek-to tx path end-val limit reverse)))

(defn seek-range
  "Provides key-value pairs ordered by key of the map at the provided `path`
  for all keys >= `start-val` and <= `end-val`.

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path start-val end-val & {:keys [limit reverse]}]
  (store/assert-txn tx)
  (let [path (prefix-path tx path)
        tx (store/view-path! tx path)]
    (ops/seek-range tx path start-val end-val limit reverse)))


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

(defmacro with-upgradable-transaction
  "Creates an upgradable transaction object and assigns it to the `tx-symbol`.
  This transaction object can be passed to any of the transaction functions.
  The `body` must evaluate to this object so it can potentially be committed.

  The transaction begins as a read transaction. If a modifying function is
  encountered the transaction may be restarted as a write transaction depending
  on if a previously read path has potentially been changed by a write in
  another transaction (Modifying functions include `assoc-at`, `update-at`,
  `merge-at`, & `dissoc-at`)

  Evaluates to nil unless a `:result-path` is supplied in which case it
  evaluates to the result of calling `(get-at tx <:result-path>)` at the
  end of the transaction.

  If another write transaction is initiated between the start of the transaction
  and an upgrade the body may be re-evaluated unless `:throw-on-upgrade` is
  true, in which case a an ExceptionInfo is thrown with the data:
  `{:codax/upgraded-transaction <tx>}`"
  ;; NOTE: if you catch this error within the upgradable transaction you MUST use
  ;; the <tx> object provided in the ex-data. This is not generally recommended and
  ;; should be regarded as experimental."
  [[database tx-symbol & {:keys [prefix result-path throw-on-upgrade] :as options}] & body]
  (assert (zero? (count (dissoc options :prefix :result-path :throw-on-upgrade))) (str "Unrecognized with-upgradable-transaction options: " (set (keys (dissoc options :prefix :result-path :throw-on-upgrade)))))
  (if (nil? result-path)
    `(store/with-upgradable-transaction [~database ~tx-symbol ~throw-on-upgrade]
       (let [~tx-symbol (set-prefix ~tx-symbol ~prefix)]
         ~@body))
    `(let [res# (atom nil)]
       (store/with-upgradable-transaction [~database ~tx-symbol ~throw-on-upgrade]
         (let [~tx-symbol (set-prefix ~tx-symbol ~prefix)
               tx-res# (do ~@body)]
           (reset! res# (get-at tx-res# ~result-path))
           tx-res#))
       @res#)))

(defmacro try-upgrade
  "Relies on Unsupported Behavior. Used for tests.

  Macro to simplify catching upgrading transactions when using the
  `with-upgradable-transaction` macro. If an upgrading transaction
  exception is caught the symbol in the catch clause is set to the
  upgraded transaction. All other exceptions are re-thrown.

  (try-upgrade
    <expression-that-might-trigger-upgrade>
    (catch <upgraded-tx-symbol>
      <forms>)
    (finally <cleanup-forms>)) ; finally clause is optional

  Example:

  (with-upgradable-transaction [db tx]
    ...
    (try-upgrade
      (assoc-at tx [:foo] 'bar)
      (catch upgraded-tx ; something must have changed
        (assoc-at upgraded-tx [:foo] 'baz))
      (finally
        (my-cleanup-function))))"
  {:deprecated "1.4.0"}
  ([expr catch-clause]
   `(try-upgrade ~expr ~catch-clause (finally)))
  ([expr catch-clause finally-clause]
   (assert (and (list? catch-clause)
                (= (first catch-clause) 'catch)
                (symbol? (second catch-clause))
                (not (empty? (rest catch-clause))))
           "catch clause must be in the form (catch upgraded-tx-symbol form & forms)")
   (assert (and (list? finally-clause)
                (= (first finally-clause) 'finally))
           "finally clause must be in the form (finally & forms)")
   `(try
      ~expr
      (catch clojure.lang.ExceptionInfo e#
        (if-let [~(second catch-clause) (:codax/upgraded-transaction (ex-data e#))]
          (do
            ~@(nthrest catch-clause 2))
          (throw e#)))
      (finally
        ~@(rest finally-clause)))))

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


(defn seek-at!
  "Wraps a `seek-at` call in a read transaction for convenience."
  [db path & {:keys [limit reverse]}]
  (with-read-transaction [db tx]
    (seek-at tx path :limit limit :reverse reverse)))

(defn seek-prefix!
  "Wraps a `seek-prefix` call in a read transaction for convenience."
  [db path val-prefix & {:keys [limit reverse]}]
  (with-read-transaction [db tx]
    (seek-prefix tx path val-prefix :limit limit :reverse reverse)))

(defn seek-prefix-range!
  "Wraps a `seek-prefix-range` call in a read transaction for convenience."
  [db path start-prefix end-prefix & {:keys [limit reverse]}]
  (with-read-transaction [db tx]
    (seek-prefix-range tx path start-prefix end-prefix :limit limit :reverse reverse)))

(defn seek-from!
  "Wraps a `seek-from` call in a read transaction for convenience."
  [db path start-val & {:keys [limit reverse]}]
  (with-read-transaction [db tx]
    (seek-from tx path start-val :limit limit :reverse reverse)))

(defn seek-to!
  "Wraps a `seek-to` call in a read transaction for convenience."
  [db path end-val & {:keys [limit reverse]}]
  (with-read-transaction [db tx]
    (seek-to tx path end-val :limit limit :reverse reverse)))

(defn seek-range!
  "Wraps a `seek-range` call in a read transaction for convenience."
  [db path start-val end-val & {:keys [limit reverse]}]
  (with-read-transaction [db tx]
    (seek-range tx path start-val end-val :limit limit :reverse reverse)))


;;;; Path Types

(defmacro defpathtype
  "defines an `encoder` and `decoder` for values of the given `types`

  `hex-code` - a numeric code to identify the type
               0x0 is used internally and is forbidden

  `types` - the types to be encoded (generally there should only be one)

  `encoder` - a function which takes elements of the types specified in `types` and
              returns a string representation

  `decoder` - a function which takes the string reprensentations generated by the
              encoder and reconstructs a value of the appropriate type

  Note: do not overwrite existing hex-code and type assignments.
        to get lists of types and hex-codes in use call `path-encoding-assignments`"
  [[hex-code & types] encoder decoder]
  `(pathwise/defpathtype [~hex-code ~@types] ~encoder ~decoder))

(defn check-path-encoding
  "encodes and decodes the provided `value` and returns a map of debugging info:

   :equal - indicates if the decoded value is equal to the initial value
   :initial - the supplied value before encoding
   :decoded - the value after it has been decoded
   :encoded - the encoded (string) representation of the value"
  [value]
  (pathwise/check-encoding value))

(defn path-encoding-assignments
  "fetches a map of types and the string representation of hex-codes presently assigned to encoders and decoders"
  [] (pathwise/encoding-assignments))
