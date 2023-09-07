(ns codax.upgrade-test
  (:require  [clojure.test :refer :all]
             [codax.core :as c]))


(def test-db-path "test-databases/upgrade-database")

(defmacro defdbtest [name db-sym & body]
  `(deftest ~name
     (c/destroy-database! test-db-path)
     (let [~db-sym (c/open-database! test-db-path)]
       (try
         (do ~@body)
         (finally
           (c/destroy-database! ~db-sym))))))

(defmacro defdbtest-fuzz [name db-sym iterations & body]
  `(deftest ~name
     (c/destroy-database! test-db-path)
     (dotimes [n# ~iterations]
       (let [~db-sym (c/open-database! test-db-path)]
         (try
           (do ~@body)
           (finally
             (c/destroy-database! ~db-sym)))))))

;;; Examples


(defdbtest basic-use-example db
  (let [maybe-update-a (fn [db value]
                         (c/with-upgradable-transaction [db tx :result-path [:change-counter]]
                           (if (= value (c/get-at tx [:a]))
                             tx
                             (-> tx
                                 (c/update-at [:change-counter] (fn [b] (if b (inc b) 1)))
                                 (c/assoc-at [:a] value)))))]


    (is (= (maybe-update-a db "hello") 1))

    (is (= (maybe-update-a db "hello") 1))

    (is (= (maybe-update-a db "world") 2))
    ))

(defdbtest conflicting-interleaved-transactions-example db
  (let [res (atom nil)]
    (is (= "ABAC"
           (with-out-str
             (let [tx1-read-checkpoint (promise)
                   tx2-write-checkpoint (promise)
                   tx1
                   (future
                     (c/with-upgradable-transaction [db tx]
                       (c/get-at tx [:somewhere]) ; read value at [:somewhere]
                       (print "A")
                       (deliver tx1-read-checkpoint :complete)
                       (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
                       (let [result-tx (c/assoc-at tx [:somewhere-else] :something)] ; try to write to [:somewhere]
                         (print "C")
                         result-tx)))
                   tx2
                   (future
                     (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
                       (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
                       (print "B")
                       (c/assoc-at tx [:somewhere] :something-else))
                     (deliver tx2-write-checkpoint :complete)
                     nil)]
               (reset! res [@tx2
                            @tx1])))))
    (is (= @res [nil nil])))

  ;; Conflict...
  ;; A. this will print twice...
  ;; B. interleave a write...
  ;; A. this will print twice...
  ;; C. but this will only print once...
  ;; => [nil nil]
  )

(defdbtest non-conflicting-interleaved-transactions-example db
  (let [res (atom nil)]
    (is (= "ABC"
           (with-out-str
             (let [
                   tx1-read-checkpoint (promise)
                   tx2-write-checkpoint (promise)
                   tx1
                   (future
                     (c/with-upgradable-transaction [db tx]
                       (c/get-at tx [:somewhere]) ; read value at [:somewhere]
                       (print "A")
                       (deliver tx1-read-checkpoint :complete)
                       (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere-else]
                       (let [result-tx (c/assoc-at tx [:somewhere] :something)]
                         (print "C")
                         result-tx)))
                   tx2
                   (future
                     (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
                       (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
                       (print "B")
                       (c/assoc-at tx [:somewhere-else] :something-else))
                     (deliver tx2-write-checkpoint :complete)
                     nil)]
               (reset! res [@tx2
                            @tx1])))))
    (is (= @res [nil nil])))

  ;; No Conflict:
  ;; A. this will only print once...
  ;; B. interleave a write...
  ;; C. and this will also only print once.
  ;; => [nil nil]
  )

(defdbtest custom-conflict-handling-example db
  (let [res (atom nil)]
    (is (= "X"
           (with-out-str
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
                           (print "X")
                           (throw e)))))
                   tx2
                   (future
                     (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
                       (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
                       (c/assoc-at tx [:somewhere] :something-else))
                     (deliver tx2-write-checkpoint :complete)
                     nil)]
               (reset! res [@tx2
                            @tx1])))))
    (is (= @res [nil nil])))

  ;; Custom Conflict Handling:
  ;; Someone else wrote to [:somewhere]...
  ;; => [nil nil]
  )

(defdbtest advanced-custom-conflict-handling-incorrect-example db
  (is (=
       (with-out-str
         (let [tx1-read-checkpoint (promise)
               tx2-write-checkpoint (promise)
               tx1
               (future
                 ;; using the `:result-path` we can have the transaction return the final value at `[:somewhere]`
                 (is (thrown-with-msg?
                      clojure.lang.ExceptionInfo #"Transaction Invalidated By Upgrade"
                      (c/with-upgradable-transaction [db tx :throw-on-upgrade true :result-path [:somewhere]]
                        (c/get-at tx [:somewhere]) ; read value at [:somewhere]
                        (deliver tx1-read-checkpoint :complete)
                        (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
                        (try ; <- try/catch within the upgradable transaction
                          (c/assoc-at tx [:somewhere] :something) ; try to write to [:somewhere]
                          (catch clojure.lang.ExceptionInfo e
                            (if (:codax/upgraded-transaction (ex-data e))
                              (do
                                (print "INCORRECT")
                                tx) ; <- because we are still within the transaction, we must return a tx
                              ;;   but this is the _wrong_ tx because we needed to swap to the one provided
                              ;;   in the exception!
                              (throw e))))))))
               tx2
               (future
                 (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
                   (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
                   (c/assoc-at tx [:somewhere] :something-else))
                 (deliver tx2-write-checkpoint :complete)
                 nil)]
           [@tx2
            @tx1]))
         ;; Someone else wrote to [:somewhere]! I'll just grab what they set.
         ;; => ExceptionInfo Transaction Invalidated By Upgrade
         ;;      {:cause :transaction-invalidated-by-upgrade
         ;;       :message "If an exception was caught because the `:throw-on-error` option was set..."}

       "INCORRECT")))

(defdbtest advanced-custom-conflict-handling-correct-example db
  (let [res (atom nil)]
    (is (= (with-out-str
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
                               (print "CORRECT")
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
               (reset! res [@tx2 @tx1])))
           "CORRECT"))
    (is (= @res [nil :something-else]))

  ;; Someone else wrote to [:somewhere]! I'll just grab what they set.
  ;; [nil :something-else]
  ))

(defdbtest fetching-a-result-example db
  (is (= 12345 (c/with-upgradable-transaction [db tx :result-path [:my-result]]
                 (c/assoc-at tx [:my-result] 12345))))

  ;; Fetching a Result:
  ;; => 12345
  )


;;; Basics

(defdbtest simple-get db
  (c/with-upgradable-transaction [db tx]
    (is (= (c/get-at tx [:something]) nil))
    tx))

(defdbtest simple-get-no-transaction db
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid Transaction"
       (c/with-upgradable-transaction [db tx]
         (is (= (c/get-at tx [:something]) nil))))))

(defdbtest simple-set-and-get db
  (is (=
       (c/with-upgradable-transaction [db tx :result-path [:somewhere]]
         (is (= (c/get-at tx [:somewhere]) nil))
         (c/assoc-at tx [:somewhere] :test-data))
       :test-data)))

(defdbtest double-upgrade db
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo #"Transaction Invalidated By Upgrade"
       (c/with-upgradable-transaction [db tx]
         (c/assoc-at tx [:somewhere] :test-data)
         (c/assoc-at tx [:somewhere] :test-data)
         tx))))

(defdbtest upgrade-view db
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo #"Transaction Invalidated By Upgrade"
       (c/with-upgradable-transaction [db tx]
         (c/assoc-at tx [:somewhere] :test-data)
         (c/get-at tx [:somewhere])
         tx))))

(defdbtest interleave-1 db
  (let [checkpoint-read (promise)
        checkpoint-write (promise)
        tx1-run-counter (atom 0)
        tx2-run-counter (atom 0)
        tx1 (future
              (c/with-upgradable-transaction [db tx :result-path [:somewhere]]
                (if (= 1 (swap! tx1-run-counter inc))
                  (is (= (c/get-at tx [:somewhere]) nil))
                  (is (= (c/get-at tx [:somewhere]) :this-should-be-both-results)))
                (deliver checkpoint-read true)
                (deref checkpoint-write)
                (c/assoc-at tx [:somewhere-else] :make-me-upgrade)))
        tx2 (future
              (c/with-upgradable-transaction [db tx :result-path [:somewhere]]
                (swap! tx2-run-counter inc)
                (deref checkpoint-read)
                (let [tx (c/assoc-at tx [:somewhere] :this-should-be-both-results)]
                  (deliver checkpoint-write true)
                  tx)))]
    (is (= @tx2 :this-should-be-both-results))
    (is (= @tx1 :this-should-be-both-results))
    (is (= @tx1-run-counter 2))
    (is (= @tx2-run-counter 1))))

(defdbtest interleave-2 db
  (let [checkpoint-read (promise)
        checkpoint-write (promise)
        tx1-run-counter (atom 0)
        tx2-run-counter (atom 0)
        tx1 (future
              (c/with-upgradable-transaction [db tx :result-path [:somewhere]]
                (if (= 1 (swap! tx1-run-counter inc))
                  (is (= (c/get-at tx [:somewhere]) nil))
                  (is (= (c/get-at tx [:somewhere]) 99)))
                (deliver checkpoint-read true)
                (deref checkpoint-write)
                (c/update-at tx [:somewhere] inc)))
        tx2 (future
              (c/with-upgradable-transaction [db tx :result-path [:somewhere]]
                (swap! tx2-run-counter inc)
                (deref checkpoint-read)
                (let [tx (c/assoc-at tx [:somewhere] 99)]
                  (deliver checkpoint-write true)
                  tx)))]
    (is (= @tx2 99))
    (is (= @tx1 100))
    (is (= @tx1-run-counter 2))
    (is (= @tx2-run-counter 1))))

(defdbtest exception-upgrade-1 db
  (is (=
       (with-out-str
         (let [tx1-read-checkpoint (promise)
               tx2-write-checkpoint (promise)
               tx1
               (future
                 (is (thrown-with-msg?
                      clojure.lang.ExceptionInfo #"Transaction Invalidated By Upgrade"
                      (c/with-upgradable-transaction [db tx :throw-on-upgrade true :result-path [:somewhere]]
                        (c/get-at tx [:somewhere]) ; read value at [:somewhere]
                        (deliver tx1-read-checkpoint :complete)
                        (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
                        (try
                          (c/assoc-at tx [:somewhere] :something) ; try to write to [:somewhere]
                          (catch clojure.lang.ExceptionInfo e
                            (if (:codax/upgraded-transaction (ex-data e))
                              (do
                                (print "Do print this")
                                (c/get-at tx [:somewhere])
                                (print "Should never print this")
                                tx)
                              (throw e))))))))
               tx2
               (future
                 (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
                   (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
                   (c/assoc-at tx [:somewhere] :something-else))
                 (deliver tx2-write-checkpoint :complete)
                 nil)]
           [@tx2
            @tx1]))
       "Do print this")))

(defdbtest exception-upgrade-2 db
  (is (=
       (with-out-str
         (let [tx1-read-checkpoint (promise)
               tx2-write-checkpoint (promise)
               tx1
               (future
                 (is (thrown-with-msg?
                      clojure.lang.ExceptionInfo #"Transaction Invalidated By Upgrade"
                      (c/with-upgradable-transaction [db tx :throw-on-upgrade true :result-path [:somewhere]]
                        (c/get-at tx [:somewhere]) ; read value at [:somewhere]
                        (deliver tx1-read-checkpoint :complete)
                        (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
                        (try
                          (c/assoc-at tx [:somewhere] :something) ; try to write to [:somewhere]
                          (catch clojure.lang.ExceptionInfo e
                            (if (:codax/upgraded-transaction (ex-data e))
                              (do
                                (print "Do print this")
                                (c/assoc-at tx [:x] :y)
                                (print "Should never print this")
                                tx)
                              (throw e))))))))
               tx2
               (future
                 (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
                   (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
                   (c/assoc-at tx [:somewhere] :something-else))
                 (deliver tx2-write-checkpoint :complete)
                 nil)]
           [@tx2
            @tx1]))
       "Do print this")))


(defdbtest exception-upgrade-3 db
  (let [res (atom nil)]
    (is (=
         (with-out-str
           (let [tx1-read-checkpoint (promise)
                 tx2-write-checkpoint (promise)
                 tx1
                 (future
                   (c/with-upgradable-transaction [db tx :throw-on-upgrade true :result-path [:x]]
                     (c/get-at tx [:somewhere]) ; read value at [:somewhere]
                     (deliver tx1-read-checkpoint :complete)
                     (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
                     (try
                       (c/assoc-at tx [:somewhere] :something) ; try to write to [:somewhere]
                       (catch clojure.lang.ExceptionInfo e
                         (if-let [tx (:codax/upgraded-transaction (ex-data e))]
                           (do
                             (print "Do print this")
                             (let [tx (c/assoc-at tx [:x] :y)]
                               (print " and this")
                               tx))
                           (throw e))))))
                 tx2
                 (future
                   (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
                     (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
                     (c/assoc-at tx [:somewhere] :something-else))
                   (deliver tx2-write-checkpoint :complete)
                   nil)]
             (reset! res [@tx2 @tx1])))
         "Do print this and this"))
    (is (= @res [nil :y]))))


(defdbtest exception-upgrade-4 db
  (let [tx1-read-checkpoint (promise)
        tx2-write-checkpoint (promise)
        tx1
        (future
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Transaction Invalidated By Upgrade")
              (c/with-upgradable-transaction [db tx :throw-on-upgrade true]
                (c/get-at tx [:somewhere]) ; read value at [:somewhere]
                (deliver tx1-read-checkpoint :complete)
                (deref tx2-write-checkpoint) ; wait for tx2 to write to [:somewhere]
                (try
                  (c/assoc-at tx [:somewhere] :something) ; try to write to [:somewhere]
                  (catch clojure.lang.ExceptionInfo e
                    (if-let [upgraded-tx (:codax/upgraded-transaction (ex-data e))]
                      tx
                      (throw e)))))))
        tx2
        (future
          (c/with-write-transaction [db tx] ; (this could also be an upgradable transaction)
            (deref tx1-read-checkpoint) ; wait for tx1 to read from [:somewhere]
            (c/assoc-at tx [:somewhere] :something-else))
          (deliver tx2-write-checkpoint :complete)
          nil)]
    [@tx2 @tx1]))

;;; Path Conflict

(defn conflict-test-helper [db path-1 path-2 should-conflict]
  (let [did-conflict (atom false)
        read-checkpoint (promise)
        write-checkpoint (promise)
        tx1 (future
              (c/with-upgradable-transaction [db tx :throw-on-upgrade true]
                (c/get-at tx path-1)
                (deliver read-checkpoint :complete)
                (when (= :failed (deref write-checkpoint 100 :failed))
                  (throw (ex-info "Failed to deref write-checkpoint" {:path-1 path-1
                                                                      :path-2 path-2})))
                (try
                  (c/assoc-at tx path-1 :value)
                  (catch clojure.lang.ExceptionInfo e
                    (if-let [tx (:codax/upgraded-transaction (ex-data e))]
                      (do
                        (reset! did-conflict true)
                        tx)
                      (throw e))))))
        tx2 (future
              (c/with-upgradable-transaction [db tx]
                (when (= :failed (deref read-checkpoint 100 :failed))
                  (throw (ex-info "Failed to deref read-checkpoint" {:path-1 path-1
                                                                     :path-2 path-2})))
                (c/assoc-at tx path-2 :value))
              (deliver write-checkpoint :complete))]
    @tx2
    @tx1
    (is (= @did-conflict should-conflict))))

(defmacro def-conflict-test [name path-1 path-2 should-conflict]
  `(deftest ~name
     (let [db# (c/open-database! test-db-path)]
       (try
         (conflict-test-helper db# ~path-1 ~path-1 true)
         (finally
           (c/destroy-database! db#))))
     (let [db# (c/open-database! test-db-path)]
       (try
         (conflict-test-helper db# ~path-2 ~path-2 true)
         (finally
           (c/destroy-database! db#))))
     (let [db# (c/open-database! test-db-path)]
       (try
         (conflict-test-helper db# ~path-1 ~path-2 ~should-conflict)
         (finally
           (c/destroy-database! db#))))
     (let [db# (c/open-database! test-db-path)]
       (try
         (conflict-test-helper db# ~path-1 ~path-2 ~should-conflict)
         (finally
           (c/destroy-database! db#))))))

(def-conflict-test conflicting-paths-1 [:a] [:a] true)
(def-conflict-test conflicting-paths-2 [:a] [:a :b] true)
(def-conflict-test conflicting-paths-3 [:a] [:a :b :c] true)
(def-conflict-test conflicting-paths-4 [:a :b] [:a :b] true)
(def-conflict-test conflicting-paths-5 [:a :b] [:a :b :c] true)
(def-conflict-test conflicting-paths-6 [:a :b :c] [:a :b :c] true)
(def-conflict-test conflicting-paths-7 [:a :b :c] [:a :b :c :d] true)
(def-conflict-test conflicting-paths-8 [:a :a] [:a] true)

(def-conflict-test non-conflicting-paths-1 [:a] [:b] false)
(def-conflict-test non-conflicting-paths-2 [:a :b] [:a :c] false)
(def-conflict-test non-conflicting-paths-3 [:a :b :c] [:a :c :b] false)
(def-conflict-test non-conflicting-paths-4 [:a :b :c :d] [:a :b :c :e] false)

(defdbtest conflicting-empty-path db
  (conflict-test-helper db [] [:a] true))

;;; Fuzz Testing

(defdbtest-fuzz fuzz-test-1 db 30
  (let [a (future
            (c/with-upgradable-transaction [db tx :result-path [:place]]
              (Thread/sleep (rand-int 10))
              (c/get-at tx [:place])
              (Thread/sleep (rand-int 10))
              (c/assoc-at tx [:place] :a)))
        b (future
            (c/with-upgradable-transaction [db tx :result-path [:place]]
              (Thread/sleep (rand-int 10))
              (c/get-at tx [:place])
              (Thread/sleep (rand-int 10))
              (c/assoc-at tx [:place] :b)))
        c (future
            (c/with-upgradable-transaction [db tx :result-path [:place]]
              (Thread/sleep (rand-int 10))
              (c/get-at tx [:place])
              (Thread/sleep (rand-int 10))
              (c/assoc-at tx [:place] :c)))
        d (future
            (c/with-upgradable-transaction [db tx :result-path [:place]]
              (Thread/sleep (rand-int 10))
              (c/get-at tx [:place])
              (Thread/sleep (rand-int 10))
              (c/assoc-at tx [:place] :d)))]
    (doall (map deref (shuffle [a b c d])))
    (is (= @a :a))
    (is (= @b :b))
    (is (= @c :c))
    (is (= @d :d))))


(defdbtest-fuzz fuzz-test-2 db 30
  (let [a-runs (atom [])
        b-runs (atom [])
        c-runs (atom [])
        d-runs (atom [])
        all-runs [a-runs b-runs c-runs d-runs]
        a (future
            (Thread/sleep (+ 10 (rand-int 10)))
            (c/with-upgradable-transaction [db tx :result-path [:place]]
              (swap! a-runs conj (c/get-at tx [:place]))
              (Thread/sleep (rand-int 10))
              (let [new-tx (c/assoc-at tx [:throwaway] :nothing)]
                (swap! a-runs conj (c/get-at new-tx [:place]))
                (c/assoc-at new-tx [:place] :a))))
        b (future
            (Thread/sleep (+ 10 (rand-int 10)))
            (c/with-upgradable-transaction [db tx :result-path [:place]]
              (swap! b-runs conj (c/get-at tx [:place]))
              (Thread/sleep (rand-int 10))
              (let [new-tx (c/assoc-at tx [:throwaway] :nothing)]
                (swap! b-runs conj (c/get-at new-tx [:place]))
                (c/assoc-at new-tx [:place] :b))))
        c (future
            (Thread/sleep (+ 10 (rand-int 10)))
            (c/with-upgradable-transaction [db tx :result-path [:place]]
              (swap! c-runs conj (c/get-at tx [:place]))
              (Thread/sleep (rand-int 10))
              (let [new-tx (c/assoc-at tx [:throwaway] :nothing)]
                (swap! c-runs conj (c/get-at new-tx [:place]))
                (c/assoc-at new-tx [:place] :c))))
        d (future
            (Thread/sleep (+ 10 (rand-int 10)))
            (c/with-upgradable-transaction [db tx :result-path [:place]]
              (swap! d-runs conj (c/get-at tx [:place]))
              (Thread/sleep (rand-int 10))
              (let [new-tx (c/assoc-at tx [:throwaway] :nothing)]
                (swap! d-runs conj (c/get-at new-tx [:place]))
                (c/assoc-at new-tx [:place] :d))))
        check-run (fn [runs self]
                    (is (<= 2 (count runs)))
                    (is (>= 3 (count runs)))
                    (when (< 2 (count runs))
                      (is (not= (get runs 0) (get runs 1)))
                      (is (= (get runs 1) (get runs 2))))
                    (is (not= (get runs 0) self))
                    (is (not= (get runs 1) self))
                    (is (not= (get runs 2) self))
                    )]

    (doall (map deref (shuffle [a b c d])))
    (is (= @a :a))
    (is (= @b :b))
    (is (= @c :c))
    (is (= @d :d))
    #_(println (map deref all-runs))
    (check-run @a-runs :a)
    (check-run @b-runs :b)
    (check-run @c-runs :c)
    (check-run @d-runs :d)
    #_(println (reduce #(+ %1 (count (deref %2))) 0 all-runs))
    (is (< (reduce #(+ % (count (deref %2))) 0 all-runs) 12))))


(defdbtest-fuzz fuzz-test-3 db 30
  (let [setup (fn [k]
                (future
                  (Thread/sleep (+ 10 (rand-int 10)))
                  (c/with-upgradable-transaction [db tx :throw-on-upgrade true :result-path [:place]]
                    (let [first-seen-value (c/get-at tx [:place])]
                      (Thread/sleep (rand-int 10))
                      (let [new-tx (try
                                     (c/assoc-at tx [:place] :k)
                                     (catch clojure.lang.ExceptionInfo e
                                       (if-let [upgraded-tx (:codax/upgraded-transaction (ex-data e))]
                                         (do
                                           (is (not= (c/get-at upgraded-tx [:place]) k))
                                           (is (not= (c/get-at upgraded-tx [:place]) first-seen-value))
                                           upgraded-tx)
                                         (throw e))))]
                        (c/assoc-at new-tx [:place] k))))))
        keys [:a :b :c :d :e :f :g :h :i :j :k :l :m :n :o :p :q :r :s :t :u :v :w :x :y :z]
        futures (mapv setup keys)
        _ (mapv deref (shuffle futures))]
    (is (= keys (mapv deref futures)))))

(defdbtest-fuzz fuzz-test-4 db 25
  (c/with-write-transaction [db tx]
    (c/assoc-at tx [:x] -1))
  (let [total 1000
        result (atom [])]
    (doall
     (pmap deref
           (for [i (shuffle (range total))]
             (future
               (c/with-upgradable-transaction [db tx]
                 (if (< (c/get-at tx [:x]) i)
                   (let [res-tx (c/assoc-at tx [:x] i)]
                     (swap! result conj i)
                     res-tx)
                   tx))))))
    (reduce (fn [acc x] (is (<= acc x)) x) -1 @result)
    #_(println @result)
    (is (= (last @result) (dec total)))))


(defdbtest-fuzz fuzz-test-5 db 25
  (c/with-write-transaction [db tx]
    (c/assoc-at tx [:x] -1))
  (let [total 1000
        result (atom [])]
    (doall
     (pmap deref
           (for [i (shuffle (range total))]
             (future
               (c/with-upgradable-transaction [db tx :throw-on-upgrade true]
                 (if (< (c/get-at tx [:x]) i)
                   (try
                     (let [tx (c/assoc-at tx [:x] i)]
                       (swap! result conj i)
                       tx)
                     (catch clojure.lang.ExceptionInfo e
                       (if-let [tx (:codax/upgraded-transaction (ex-data e))]
                         (if (< (c/get-at tx [:x]) i)
                           (let [tx (c/assoc-at tx [:x] i)]
                             (swap! result conj i)
                             tx)
                           tx)
                         (throw e))))
                   tx))))))
    (reduce (fn [acc x] (is (<= acc x)) x) -1 @result)
    #_(println @result)
    (is (= (last @result) (dec total)))))

(defdbtest-fuzz fuzz-test-6 db 25
  (c/with-write-transaction [db tx]
    (c/assoc-at tx [:x] -1))
  (let [total 1000
        result (atom [])]
    (doall
     (pmap deref
           (for [i (shuffle (range total))]
             (future
               (c/with-upgradable-transaction [db tx :throw-on-upgrade true]
                 (if (< (c/get-at tx [:x]) i)
                   (c/try-upgrade
                    (let [tx (c/assoc-at tx [:x] i)]
                      (swap! result conj i)
                      tx)
                    (catch tx
                        (if (< (c/get-at tx [:x]) i)
                          (let [tx (c/assoc-at tx [:x] i)]
                            (swap! result conj i)
                            tx)
                          tx)))
                   tx))))))
    (reduce (fn [acc x] (is (<= acc x)) x) -1 @result)
    #_(println @result)
    (is (= (last @result) (dec total)))))
