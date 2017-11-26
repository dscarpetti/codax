(ns codax.core-test
  (:require [clojure.test :refer :all]
            [codax.test-logging :refer [logln]]
            [codax.core :refer :all]
            [codax.store :refer [destroy-database]]
            [codax.swaps :refer :all]))

(def ^:dynamic *testing-database* nil)

(defn store-setup-and-teardown [f]
  (binding [*testing-database* (open-database! "test-databases/core")]
                                        ;(logln "SETUP")
    (f))
  (destroy-database "test-databases/core"))

(use-fixtures :each store-setup-and-teardown)

(defmacro wrc [write-tx read-tx final-value]
  `(is
    (=
     (do
       (with-write-transaction [~'*testing-database* ~'tx]
         ~write-tx)
       (let [x#
             (with-read-transaction [~'*testing-database* ~'tx]
               ~read-tx)]
         ;;(logln x#)
         x#))
     ~final-value)))

(defmacro rc [read-tx final-value]
  `(is
    (=
     (let [x#
           (with-read-transaction [~'*testing-database* ~'tx]
             ~read-tx)]
       ;;(logln x#)
       x#)
     ~final-value)))


(deftest single-assoc-at
  (wrc
   (assoc-at tx [1 "a" :b 1.0 'c] ["x"])
   (get-at tx [1 "a" :b 1.0 'c])
   ["x"])
  (rc
   (get-at tx [1 "a" :b 1.0])
   {'c ["x"]})
  (rc
   (get-at tx [1 "a" :b])
   {1.0 {'c ["x"]}})
  (rc
   (get-at tx [1 "a"])
   {:b {1.0 {'c ["x"]}}})
  (rc
   (get-at tx [1])
   {"a" {:b {1.0 {'c ["x"]}}}})
  (rc
   (get-at tx [])
   {1 {"a" {:b {1.0 {'c ["x"]}}}}})
  (rc
   (get-at tx nil)
   {1 {"a" {:b {1.0 {'c ["x"]}}}}}))

(deftest assoc-at+dissoc-at
  (wrc
   (assoc-at tx [-1 :people] {"Sam" {:name "Sam"
                                     :title "Mr"}
                              "Sally" {:name "Sally"
                                       :title "Mrs"}})
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"
    :title "Mr"})
  (rc
   (get-at tx [-1])
   {:people {"Sam" {:name "Sam"
                    :title "Mr"}
             "Sally" {:name "Sally"
                      :title "Mrs"}}})
  (wrc
   (dissoc-at tx [-1 :people "Sam" :title])
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"})
  (wrc
   (dissoc-at tx [-1 :people "Sam"])
   (get-at tx [-1 :people "Sam"])
   nil)
  (rc
   (get-at tx [-1])
   {:people {"Sally" {:name "Sally"
                      :title "Mrs"}}}))

(deftest assoc-at+assoc-at-overwrite
  (wrc
   (assoc-at tx [-1 :people] {"Sam" {:name "Sam"
                                     :title "Mr"}
                              "Sally" {:name "Sally"
                                       :title "Mrs"}})
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"
    :title "Mr"})
  (rc
   (get-at tx [-1])
   {:people {"Sam" {:name "Sam"
                    :title "Mr"}
             "Sally" {:name "Sally"
                      :title "Mrs"}}})
  (wrc
   (assoc-at tx [-1 :people "Sam" :title] "Sir")
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"
    :title "Sir"})
  (wrc
   (assoc-at tx [-1 :people "Sam"] {:name "Sammy" :profession "Go"})
   (get-at tx [-1 :people "Sam"])
   {:name "Sammy"
    :profession "Go"})
  (rc
   (get-at tx [-1])
   {:people {"Sally" {:name "Sally"
                      :title "Mrs"}
             "Sam" {:name "Sammy"
                    :profession "Go"}}}))

(deftest assoc-at+merge-at-overwrite
  (wrc
   (assoc-at tx [-1 :people] {"Sam" {:name "Sam"
                                     :title "Mr"}
                              "Sally" {:name "Sally"
                                       :title "Mrs"}})
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"
    :title "Mr"})
  (rc
   (get-at tx [-1])
   {:people {"Sam" {:name "Sam"
                    :title "Mr"}
             "Sally" {:name "Sally"
                      :title "Mrs"}}})
  (wrc
   (merge-at tx [-1 :people "Sam"] {:title "Sir"})
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"
    :title "Sir"})
  (wrc
   (merge-at tx [-1 :people "Sam"] {:name "Sammy" :profession "Go"})
   (get-at tx [-1 :people "Sam"])
   {:name "Sammy"
    :title "Sir"
    :profession "Go"})
  (rc
   (get-at tx [-1])
   {:people {"Sally" {:name "Sally"
                      :title "Mrs"}
             "Sam" {:name "Sammy"
                    :title "Sir"
                    :profession "Go"}}}))

;;;;;


(deftest one-write-assoc-at+dissoc-at
  (wrc
   (-> tx
       (assoc-at [-1 :people] {"Sam" {:name "Sam"
                                      :title "Mr"}
                               "Sally" {:name "Sally"
                                        :title "Mrs"}})
       (dissoc-at [-1 :people "Sam" :title])
       (dissoc-at [-1 :people "Sam"]))
   (get-at tx [-1])
   {:people {"Sally" {:name "Sally"
                      :title "Mrs"}}}))

(deftest one-write-assoc-at+assoc-at-overwrite
  (wrc
   (-> tx
       (assoc-at [-1 :people] {"Sam" {:name "Sam"
                                      :title "Mr"}
                               "Sally" {:name "Sally"
                                        :title "Mrs"}})
       (assoc-at [-1 :people "Sam" :title] "Sir")
       (assoc-at [-1 :people "Sam"] {:name "Sammy" :profession "Go"}))
   (get-at tx [-1])
   {:people {"Sally" {:name "Sally"
                      :title "Mrs"}
             "Sam" {:name "Sammy"
                    :profession "Go"}}}))

(deftest one-write-assoc-at+merge-at
  (wrc
   (-> tx
       (assoc-at [-1 :people] {"Sam" {:name "Sam"
                                      :title "Mr"}
                               "Sally" {:name "Sally"
                                        :title "Mrs"}})
       (merge-at [-1 :people "Sam"] {:title "Sir"})
       (merge-at [-1 :people "Sam"] {:name "Sammy" :profession "Go"}))
   (get-at tx [-1])
   {:people {"Sally" {:name "Sally"
                      :title "Mrs"}
             "Sam" {:name "Sammy"
                    :title "Sir"
                    :profession "Go"}}}))

(deftest write-transaction-without-final-tx
  (is
   (thrown-with-msg?
    clojure.lang.ExceptionInfo #"Invalid Transaction"
    (with-write-transaction [*testing-database* tx]))))

(deftest write-transaction-with-invalid-database-nil
  (is
   (thrown-with-msg?
    clojure.lang.ExceptionInfo #"Invalid Database"
    (with-write-transaction [nil tx]))))

(deftest write-transaction-with-invalid-database-2
  (is
   (thrown-with-msg?
    clojure.lang.ExceptionInfo #"Invalid Database"
    (with-write-transaction [2 tx]))))

(deftest write-transaction-with-invalid-database-map
  (is
   (thrown-with-msg?
    clojure.lang.ExceptionInfo #"Invalid Database"
    (with-write-transaction [{} tx]))))

(deftest assoc-at-without-txn
  (is
   (thrown-with-msg?
    clojure.lang.ExceptionInfo #"Invalid Transaction"
    (assoc-at "no-tx" [:a] "something"))))

(deftest failed-assoc-empty-path
  (try
    (with-write-transaction [*testing-database* tx]
      (assoc-at tx [] "failure"))
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [cause] :as data} (ex-data e)]
        (logln data)
        (is (= (.getMessage e) "Invalid Path"))
        (is (= cause :empty-path))))))

(deftest failed-assoc+extended-past-existing
  (try
    (with-write-transaction [*testing-database* tx]
      (-> tx
          (assoc-at [:hello] "value here")
          (assoc-at [:hello :world] "failure")))
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [cause] :as data} (ex-data e)]
        (logln data)
        (is (= (.getMessage e) "Occupied Path"))
        (is (= cause :non-map-element))))))


(deftest failed-assoc+extended-past-existing-separate-transactions
  (try
    (do
      (with-write-transaction [*testing-database* tx]
        (assoc-at tx [:something] "value here"))
      (with-write-transaction [*testing-database* tx]
        (assoc-at tx [:hello :world] "failure")))
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [cause] :as data} (ex-data e)]
        (logln data)
        (is (= (.getMessage e) "Occupied Path"))
        (is (= cause :non-map-element))))))


(defn increment-path [db]
  (with-write-transaction [db tx]
    (update-at tx [:metrics :user-counts :all] inc-count)))

(defn increment-test [db n]
  (let [database db
        ops (repeat n #(increment-path database))]
    (doall (pmap #(%) ops))
    (is (=
         n
         (with-read-transaction [database tx]
           (get-at tx [:metrics :user-counts :all]))))))

(deftest inc-test
  (increment-test *testing-database* 1000))


;;;; convenience function tests

(deftest assoc-at!-test
  (is (=
       (assoc-at! *testing-database* [:letters] {:a 1 :b 2})
       (get-at! *testing-database* [:letters])
       {:a 1 :b 2}))
  (is (=
       (get-at! *testing-database*)
       {:letters {:a 1 :b 2}})))


(deftest update-at!-test
  (let [add (fn [x y] (+ x y))
        subtract (fn [x y] (- x y))]
    (is (= 1 (update-at! *testing-database* [:count] inc-count)))
    (is (= 2 (update-at! *testing-database* [:count] inc-count)))
    (is (= 12 (update-at! *testing-database* [:count] add 10)))
    (is (= 7 (update-at! *testing-database* [:count] subtract 5)))
    (is (= 7 (get-at! *testing-database* [:count])))))

(deftest merge-at!-test
  (is (=
       (assoc-at! *testing-database* [:letters] {:a 1 :b 2})
       (get-at! *testing-database* [:letters])
       {:a 1 :b 2}))
  (is (=
       (merge-at! *testing-database* [:letters] {:c 3 :d 4})
       (get-at! *testing-database* [:letters])
       {:a 1 :b 2 :c 3 :d 4}))
  (is (=
       (get-at! *testing-database*)
       {:letters {:a 1 :b 2 :c 3 :d 4}})))

(deftest dissoc-at!-test
  (is (=
       (assoc-at! *testing-database* [:letters] {:a 1 :b 2 :c 3})
       (get-at! *testing-database* [:letters])
       {:a 1 :b 2 :c 3}))
  (is (=
       (dissoc-at! *testing-database* [:letters :c])
       nil))
  (is (=
       (get-at! *testing-database* [:letters])
       {:a 1 :b 2}))
  (is (=
       (dissoc-at! *testing-database* [:letters])
       (get-at! *testing-database* [:letters])
       nil))
  (is (=
       (get-at! *testing-database*)
       {})))

(deftest is-open?-and-close-all
  (is (not (is-open? "test-databases/coreA")))
  (is (not (is-open? "test-databases/coreB")))
  (is (not (is-open? "test-databases/coreC")))

  (let [a (open-database! "test-databases/coreA")
        b (open-database! "test-databases/coreB")
        c (open-database! "test-databases/coreC")]

    (is (is-open? a))
    (is (is-open? b))
    (is (is-open? c))
    (is (is-open? "test-databases/coreA"))
    (is (is-open? "test-databases/coreB"))
    (is (is-open? "test-databases/coreC"))

    (close-all-databases!)

    (is (not (is-open? a)))
    (is (not (is-open? b)))
    (is (not (is-open? c)))
    (is (not (is-open? "test-databases/coreA")))
    (is (not (is-open? "test-databases/coreB")))
    (is (not (is-open? "test-databases/coreC")))))

;; -- consistency validation --

(deftest extend-nil-path-with-val
  (is (=
       (assoc-at! *testing-database* [:a] nil)
       nil))
  (is (=
       (assoc-at! *testing-database* [:a :b] "val")
       "val"))
  (is (=
       (get-at! *testing-database*)
       {:a {:b "val"}})))

(deftest extend-nil-path-with-map
  (is (=
       (assoc-at! *testing-database* [:a] nil)
       nil))
  (is (=
       (assoc-at! *testing-database* [:a] {:b "val"})
       {:b "val"}))
  (is (=
       (get-at! *testing-database*)
       {:a {:b "val"}})))

(deftest extend-val-path-with-val
  (is (=
       (assoc-at! *testing-database* [:a] "anything")
       "anything"))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Occupied Path"
                        (assoc-at! *testing-database* [:a :b] "val")))
  (is (=
       (get-at! *testing-database*)
       {:a "anything"})))

(deftest extend-val-path-with-map
  (is (=
       (assoc-at! *testing-database* [:a] "anything")
       "anything"))
  (is (=
       (assoc-at! *testing-database* [:a] {:b "val"})
       {:b "val"}))
  (is (=
       (get-at! *testing-database*)
       {:a {:b "val"}})))

(deftest extend-val-path-with-val-long
  (is (=
       (assoc-at! *testing-database* [:a :b] "anything")
       "anything"))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Occupied Path"
                        (assoc-at! *testing-database* [:a :b :c] "val")))
  (is (=
       (get-at! *testing-database*)
       {:a {:b "anything"}})))

(deftest extend-val-path-with-partial-val-long
  (is (=
       (assoc-at! *testing-database* [:a] "anything")
       "anything"))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Occupied Path"
                        (assoc-at! *testing-database* [:a :b :c] "val")))
  (is (=
       (get-at! *testing-database*)
       {:a "anything"})))

(deftest extend-val-path-with-partial-map-long
  (is (=
       (assoc-at! *testing-database* [:a :b] "anything")
       "anything"))
  (is (=
       (assoc-at! *testing-database* [:a] {:b {:c "val"}})
       {:b {:c "val"}}))
  (is (=
       (get-at! *testing-database*)
       {:a {:b {:c "val"}}})))

(deftest extend-val-path-with-map-long
  (is (=
       (assoc-at! *testing-database* [:a :b] "anything")
       "anything"))
  (is (=
       (assoc-at! *testing-database* [:a :b] {:c "val"})
       {:c "val"}))
  (is (=
       (get-at! *testing-database*)
       {:a {:b {:c "val"}}})))

(deftest extend-false-path-with-val
  (is (=
       (assoc-at! *testing-database* [:a] false)
       false))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Occupied Path"
                        (assoc-at! *testing-database* [:a :b] "val")))
  (is (=
       (get-at! *testing-database*)
       {:a false})))

(deftest extend-false-path-with-map
  (is (=
       (assoc-at! *testing-database* [:a] false)
       false))
  (is (=
       (assoc-at! *testing-database* [:a] {:b "val"})
       {:b "val"}))
  (is (=
       (get-at! *testing-database*)
       {:a {:b "val"}})))

(deftest extend-vec-path-with-val
  (is (=
       (assoc-at! *testing-database* [:a] [])
       []))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Occupied Path"
                        (assoc-at! *testing-database* [:a :b] "val")))
  (is (=
       (get-at! *testing-database*)
       {:a []})))

(deftest extend-vec-path-with-map
  (is (=
       (assoc-at! *testing-database* [:a] [])
       []))
  (is (=
       (assoc-at! *testing-database* [:a] {:b "val"})
       {:b "val"}))
  (is (=
       (get-at! *testing-database*)
       {:a {:b "val"}})))

;; --- path conversions ---

(deftest path-conversion-from-val
  (is (=
       (assoc-at! *testing-database* :foo "bar")
       "bar"))
  (is (=
       (get-at! *testing-database*))
      {:foo "bar"}))

(deftest path-conversion-from-list
  (is (=
       (assoc-at! *testing-database* (list :foo :bar) "baz")
       "baz"))
  (is (=
       (get-at! *testing-database*))
      {:foo {:bar "baz"}}))

(deftest path-conversion-from-seq
  (is (=
       (assoc-at! *testing-database* (map identity [:foo :bar]) "baz")
       "baz"))
  (is (=
       (get-at! *testing-database*))
      {:foo {:bar "baz"}}))
