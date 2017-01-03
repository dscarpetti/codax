(ns codax.core
  (:require
   [codax.operations :as ops]
   [codax.prefix :refer [set-prefix prefix-path]]
   [codax.store :as store]));; :refer [open-database list-all-databases close-database]]))

(defn open-database [filepath]
  (store/open-database filepath))

(defn close-database [filepath-or-db]
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

  Supplying `nil` for `val-or-map` is the equivalet of running `dissoc-at`.
  Nested `nil` values are not stored.

  The values will be changed in the datastore when the transaction is committed
  (occurs  after the `body` of a `with-write-transaction` form).

  All paths will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path val-or-map]
  (ops/assoc-map tx (prefix-path tx path) val-or-map))

(defn update-at
  "Runs a function on the current map or value at the supplied path and `assoc-at`s the result.

  If the result of evaluation is `nil` it is the equivalet of running `dissoc-at`.
  Nested `nil` values are not stored.

  The values will be changed in the datastore when the transaction is committed
  (occurs  after the `body` of a `with-write-transaction` form).

  All paths will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path f & args]
  (apply ops/update-map tx (prefix-path tx path) f args))

(defn dissoc-at
  "Deletes all values at the supplied path.

  The value will be deleted from the datastore when the transaction is committed, (which
  (occurs  after the `body` of a `with-write-transaction` form).

  Equivalent to calling `assoc-at` with a `val-or-map` of `nil`.

  The path will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path]
  (ops/delete-map tx (prefix-path tx path)))

(defn search
  "Iterates through the datastore and collects a vector of results in the form:
  `[{:key ~key :val ~val} ...]`
  where:
  * ~key is the immediate descendant key or matched key in the case of `:partial`)
  * ~val is the value at `(conj path key)`

  Iteration begins at `path` and moves forward (by default).

  Options:

  If an `:until` path fragment (atom or vector) is supplied, the iteration will stop at:
  `(concat from until)`
  or, if `until` is an atom:
  `(conj from until)`

  If the `:partial` flag is set the *last value* in the `path` is treated as incomplete and
  all paths that begin with it will be collected. `:to-suffix` will be ignored in this case.

  `:limit` is a number which determines how many result to return. If unset, all values will
  be returned.

  `:reverse` inverts the direction of iteration.

  Ex. If the transaction `tx` has no prefix & the database contains:

  [ key ]                   | val
  -------------------------------
  [:dept :art      :alice]  |  1
  [:dept :bio-tech   :bob]  |  2
  [:dept :biology  :chloe]  |  3
  [:dept :biology :daniel]  |  4
  [:dept :biology  :ellen]  |  5
  [:dept :botany   :frank]  |  6
  -------------------------------

  ```
  (search tx [:dept]) ->

  [{:key :art      :val {:alice 1}}
   {:key :bio-tech :val {:bob 2}}
   {:key :biology  :val {:chloe 3, :daniel 4, :ellen 5}
   {:key :botany   :val {:frank 6}}]

  ---

  (search tx [:dept] :limit 2) ->

  [{:key :art      :val {:alice 1}}
   {:key :bio-tech :val {:bob 2}}]

  ---

  (search tx [:dept :biology]) ->

  [{:key :chloe  :val 3}
   {:key :daniel  :val 4}
   {:key :ellen  :val 5}]

  ---

  (search tx [:dept :biology] :reverse true) ->

  [{:key :ellen  :val 5}
   {:key :daniel  :val 4}
   {:key :chloe  :val 3}]

  ---

  (search tx [:dept :biology] :reverse true :limit 2) ->

  [{:key :ellen  :val 5}
   {:key :daniel  :val 4}]

  ---

  (search tx [:dept :biology] :until [:daniel]) ->

  [{:key :ellen  :val 5}
   {:key :daniel  :val 4}]

  ---

  (search tx [:dept :biology :chloe]) ->

  [{:key nil  :val 3}]

  ---

  (search tx [:dept :bi] :partial true) ->

  [{:key :bio-tech :val {:bob 2}}
   {:key :biology  :val {:chloe 3 :daniel 4 :ellen 5}}]

  ---"
  [tx path & {:keys [until limit reverse partial]}]
  (ops/search tx (prefix-path tx path) :to-suffix until :limit limit :reverse reverse :partial partial))

;; Put operations

(defn put
  "Puts a value, or map of values, at the supplied path.

  If the value is `nil`, or is a map with `nil` values, the value(s) at the respective
  locations will be deleted. Missing values will not.

  If the value is anything else, it will be inserted into the database at the supplied
  path, overwriting any existing value.

  The values will be changed in the datastore when the transaction is committed
  (occurs  after the `body` of a `with-write-transaction` form).

  All paths will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path val-or-map]
  (ops/put-map tx (prefix-path tx path) val-or-map))

(defn put-update
  "Runs a function on the current map or value at the supplied path and `put`s the result.

  If the value is `nil`, or is a map with `nil` values, the value(s) at the respective
  locations will be deleted. Missing values will not.

  If the value is anything else, it will be inserted into the database at the supplied
  path, overwriting any existing value.

  Note: because `put` is used, a result map that is missing keys will *not* remove those
  keys. Keys must have a value of `nil` to be removed. So, in general `dissoc` will have
  no effect.

  The values will be changed in the datastore when the transaction is committed
  (occurs  after the `body` of a `with-write-transaction` form).

  All paths will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not)."
  [tx path f & args]
  (apply ops/put-map-update tx (prefix-path tx path) f args))


;; Value Operations

(defn get-val
  "Returns the value at the supplied `path`. If the value has been modified within the
  transaction, it will evaluate to the modified value

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not).

  *Warning*
  `get-at` is preferred, unless you are certain the path does not point to a map. If it
  does, nothing will be returned."
  [tx path]
  (ops/get-val tx (prefix-path tx path)))


(defn put-val
  "Puts a non-map value `v` at `path`. If the value is `nil` is is the equivalent of
  calling `delete-val`

  The value will be set in the datastore when the transaction is committed, (which
  (occurs  after the `body` of a `with-write-transaction` form).

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not).

  *Warning*
  `put` is preferred, unless you are certain the path does not point to a map. If it
  does, the datastore will be left in an inconsistent state."
  [tx path v]
  (assert (not (map? v)) "The value cannot be a map")
  (ops/put-val tx (prefix-path tx path) v))

(defn update-val
  "Runs the supplied function current value at `path`, and set the `path` to the result.
  The value will be set in the datastore when the transaction is committed, (which
  (occurs  after the `body` of a `with-write-transaction` form).

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not).

  *Warning*
  `update-at` is preferred, unless you are certain the path does not point to a map. If it
  does, the datastore will be left in an inconsistent state."
  [tx path f & args]
  (apply ops/update-val tx (prefix-path tx path) f args))

(defn delete-val
  "Clears the non-map value at `path`.

  The value will be deleted from the datastore when the transaction is committed, (which
  (occurs  after the `body` of a `with-write-transaction` form).

  `path` will be prefixed with the transaction `prefix` (if the transaction has one,
  by default, it does not).

  *Warning*
  `dissoc-at` is preferred, unless you are certain that the path does not point
  to a map. If it does, nothing will occur."
  [tx path]
  (ops/delete-val tx (prefix-path tx path)))

;;;; Transactions

(defmacro with-write-transaction
  "Creates a write transaction object and assigns it to the `tx-symbol`.
  This transaction object can be passed to any of the transaction functions.
  The `body` must evaluate to this object so it can be committed."
  [[database tx-symbol & {:keys [prefix]}] & body]
  `(let [db# ~database]
     (locking (:lock-obj db#)
       (let [~tx-symbol (set-prefix (store/transaction db#) ~prefix)]
         (store/commit! (do ~@body))))))

(defmacro with-read-transaction
  "Creates a read transaction object and assigns it to `tx-symbol`.
  This transaction object can be passed to the non-modifying transaction functions.
  Behavior when passed to a modifying transaction function is undefined.
  The final value will be the result of evaluating the `body`."
  [[database tx-symbol & {:keys [prefix]}] & body]
  `(let [db# ~database
         ~tx-symbol (set-prefix (store/transaction db#) ~prefix)]
       (store/with-read-lock [db#] ~@body)))
