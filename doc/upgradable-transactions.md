# Upgradable Transactions (added in version 1.4.0)

## with-upgradable-transaction

Upgradable transactions begin as read transaction but upgrade into write transaction when a function is called that would modify the database. If possible, this upgrade is performed seamlessly. However, if prior to upgrading the transaction reads from a `path` that has since been (potentially) modified by another concurrent transaction, the `body` of the transaction will be restarted from the beginning (as a write transaction). This behavior can be overridden by setting the `:throw-on-upgrade` option (details below).

To create an upgradable transaction you use the `with-upgradable-transaction` macro which has the form `([[database tx-symbol & {:keys [prefix result-path throw-on-upgrade]}] & body])`. This is similar to the `with-read-transaction` & `with-write-transaction` macros with two additional optional keyword arguments:
  - `:result-path` - if this option is supplied the transaction will return the result of calling `(get-at tx <:result-path>)` after the `body` is executed (but within the scope of the transaction. If this option is not supplied, `with-upgradable-transaction` evaluates to `nil`
  - `:throw-on-upgrade` - if this option is truthy the transaction will not be automatically restarted if a conflict is detected. Instead an `clojure.lang.ExceptionInfo` will be thrown that contains the ex-data: `{:codax/upgraded-transaction <tx>}`.
    - If you intend to catch the exception it is recommended that you do so outside the scope of the transaction and initiate a new transaction.
    - If you do catch the exception *within* the upgradable transaction you **must continue with or return the upgraded transaction** supplied in the `ex-data` of the ExceptionInfo at the `:codax/upgraded-transaction` key. For example: `(let [upgraded-tx (:codax/upgraded-transaction (ex-data e)] (-> upgraded-tx ...)`.

Modification functions that will trigger an upgrade from a read to a write transaction include:
  - `assoc-at`
  - `update-at`
  - `merge-at`
  - `dissoc-at`

## Examples

### Basic Use

``` clojure
(def db (c/open-database! "data/demo-database"))

(defn maybe-update-a
  "If the value at `[:a]` in `db` is not= `value` then set it to `value`
  and increment the `:change-counter`.

  Return the final value of `:change-counter`"
  [db value]
  (c/with-upgradable-transaction [db tx :result-path [:change-counter]]
    (if (= value (c/get-at tx [:a]))
      tx
      (-> tx
        (c/update-at [:change-counter] (fn [b] (if b (inc b) 1)))
        (c/assoc-at [:a] value)))))

(def db (c/open-database! "data/demo-database"))

(maybe-update-a db "hello")
;; => 1

(maybe-update-a db "hello")
;; => 1

(maybe-update-a db "world")
;; => 2

(c/destroy-database! db)
```

### Fetching a Result

``` clojure
(def db (c/open-database! "data/demo-database"))

(c/with-upgradable-transaction [db tx :result-path [:my-result]]
  (c/assoc-at tx [:my-result] 12345)))

;; => 12345

(c/destroy-database! db)
```

### Conflicting Interleaved Transactions

``` clojure
(def db (c/open-database! "data/demo-database"))

(let [tx1-read-checkpoint (promise)
      tx2-write-checkpoint (promise)
      tx1
      (future
        (c/with-upgradable-transaction [db tx]
          (c/get-at tx [:somewhere]) ; read value at [:somewhere]
          (print "A. this will print twice because the transaction is restarted when it attempts")
          (print " to upgrade because the interleaved write transaction modified a path, [:something],")
          (println " that this transaction had already read." )
          (deliver tx1-read-checkpoint :complete)
          (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
          (let [result-tx (c/assoc-at tx [:somewhere-else] :something)] ; try to write to [:somewhere]
            (println "C. but this will only print once because it occurs after the transaction has upgraded")
            result-tx)))
      tx2
      (future
        (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
          (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
          (println "B. interleave a write that conflicts with the other transaction's previous read")
          (c/assoc-at tx [:somewhere] :something-else))
        (deliver tx2-write-checkpoint :complete)
        nil)]
  [@tx2
   @tx1])

;; A. this will print twice...
;; B. interleave a write...
;; A. this will print twice...
;; C. but this will only print once...
;; => [nil nil]

(c/destroy-database! db)
```

### Non-conflicting Interleaved Transactions

``` clojure
(def db (c/open-database! "data/demo-database"))

(let [tx1-read-checkpoint (promise)
      tx2-write-checkpoint (promise)
      tx1
      (future
        (c/with-upgradable-transaction [db tx]
          (c/get-at tx [:somewhere]) ; read value at [:somewhere]
          (println "A. this will only print once since there is no conflict.")
          (deliver tx1-read-checkpoint :complete)
          (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere-else]
          (let [result-tx (c/assoc-at tx [:somewhere] :something)]
            (println "C. and this will also only print once.")
            result-tx)))
      tx2
      (future
        (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
          (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
          (println "B. interleave an unrelated write")
          (c/assoc-at tx [:somewhere-else] :something-else))
        (deliver tx2-write-checkpoint :complete)
        nil)]
  [@tx2
   @tx1])

;; A. this will only print once...
;; B. interleave a write...
;; C. and this will also only print once.
;; => [nil nil]

(c/destroy-database! db)
```

### Custom Conflict Handling

``` clojure
(def db (c/open-database! "data/demo-database"))

(let [tx1-read-checkpoint (promise)
      tx2-write-checkpoint (promise)
      tx1
      (future
        (try
          (c/with-upgradable-transaction [db tx :throw-on-upgrade true]
            (c/get-at tx [:somewhere]) ; read value at [:somewhere]
            (deliver tx1-read-checkpoint :complete)
            (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
            (c/assoc-at tx [:somewhere] :something)) ; try to write to [:somewhere]
          (catch clojure.lang.ExceptionInfo e
            (if (:codax/upgraded-transaction (ex-data e))
              (println "Someone else wrote to [:somewhere]! I'll just give up and do nothing")
              (throw e)))))
      tx2
      (future
        (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
          (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
          (c/assoc-at tx [:somewhere] :something-else))
        (deliver tx2-write-checkpoint :complete)
        nil)]
  [@tx2
   @tx1])

;; Someone else wrote to [:somewhere]...
;; => [nil nil]

(c/destroy-database! db)
```

### Advanced Custom Conflict Handling

We can catch the exception within the transaction block but we must be sure to use the upgraded transaction supplied in the ex-data of the exception.

_Incorrect:_

``` clojure
(def db (c/open-database! "data/demo-database"))

(let [tx1-read-checkpoint (promise)
      tx2-write-checkpoint (promise)
      tx1
      (future
        ;; using the `:result-path` we can have the transaction return the final value at `[:somewhere]`
        (c/with-upgradable-transaction [db tx :throw-on-upgrade true :result-path [:somewhere]]
          (c/get-at tx [:somewhere]) ; read value at [:somewhere]
          (deliver tx1-read-checkpoint :complete)
          (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
          (try ; <- try/catch within the upgradable transaction
            (c/assoc-at tx [:somewhere] :something) ; try to write to [:somewhere]
            (catch clojure.lang.ExceptionInfo e
              (if (:codax/upgraded-transaction (ex-data e))
                (do
                  (println "Someone else wrote to [:somewhere]! I'll just grab what they set.")
                  tx) ; <- because we are still within the transaction, we must return a tx
                      ;;   but this is the _wrong_ tx because we needed to swap to the one provided
                      ;;   in the exception!
                (throw e))))))
      tx2
      (future
        (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
          (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
          (c/assoc-at tx [:somewhere] :something-else))
        (deliver tx2-write-checkpoint :complete)
        nil)]
  [@tx2
   @tx1])

;; Someone else wrote to [:somewhere]! I'll just grab what they set.
;; => ExceptionInfo Transaction Invalidated By Upgrade
;;      {:cause :transaction-invalidated-by-upgrade
;;       :message "Once a transaction is upgraded the previous tx object is invalid...."}


(c/destroy-database! db)
```

_Correct:_

``` clojure
(def db (c/open-database! "data/demo-database"))

(let [tx1-read-checkpoint (promise)
      tx2-write-checkpoint (promise)
      tx1
      (future
        ;; using the `:result-path` we can have the transaction return the final value at `[:somewhere]`
        (c/with-upgradable-transaction [db tx :throw-on-upgrade true :result-path [:somewhere]]
          (c/get-at tx [:somewhere]) ; read value at [:somewhere]
          (deliver tx1-read-checkpoint :complete)
          (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
          (try ; <- try/catch within the upgradable transaction
            (c/assoc-at tx [:somewhere] :something) ; try to write to [:somewhere]
            (catch clojure.lang.ExceptionInfo e
              (if-let [tx (:codax/upgraded-transaction (ex-data e))]
                (do
                  (println "Someone else wrote to [:somewhere]! I'll just grab what they set.")
                  tx) ; <- because we are still within the transaction, we must return a tx
                      ;;   but now we've replaced it with the one returned by the exception
                      ;;   so we are good to go!
                (throw e))))))
      tx2
      (future
        (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
          (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
          (c/assoc-at tx [:somewhere] :something-else))
        (deliver tx2-write-checkpoint :complete)
        nil)]
  [@tx2
   @tx1])

;; Someone else wrote to [:somewhere]! I'll just grab what they set.
;; [nil :something-else]


(c/destroy-database! db)
```

## try-upgrade

The `try-upgrade` macro simplifies the basic case of handling upgrades manually within an upgradable transaction using the `:throw-on-upgrade` option. *It should only be used within the body of a `with-upgradable-transaction` form. It is a simple helper to reduce boilerplate and hopefully increase code clarity. It only catches upgrade exceptions, all other exceptions are rethrown.

It lets this:

``` clojure
(c/with-upgradable-transaction [db tx]
  ...
  (try
    (assoc-at tx [:somewhere] :something)
    (catch clojure.lang.ExceptionInfo e
      (if-let [upgraded-tx (:codax/upgraded-transaction (ex-data e))]
        (do
          (form)
          (form))
        (throw e)))))
```

be expressed more concisely as this:


``` clojure
(c/with-upgradable-transaction [db tx]
  ...
  (c/try-upgrade
    (assoc-at tx [:somewhere] :something)
    (catch upgraded-tx
      (form)
      (form))))
```

See the docstring for additional details.
