(ns codax.core-test
  (:require [clojure.test :refer :all]
            [codax.core :refer :all]
            [codax.store :refer [destroy-database]]))

(def ^:dynamic *testing-database* nil)

(defn store-setup-and-teardown [f]
  (binding [*testing-database* (open-database "test-databases/core")]
                                        ;(println "SETUP")
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
         ;;(println x#)
         x#))
     ~final-value)))

(defmacro rc [read-tx final-value]
  `(is
    (=
     (let [x#
           (with-read-transaction [~'*testing-database* ~'tx]
             ~read-tx)]
       ;;(println x#)
       x#)
     ~final-value)))


(deftest put-single-val
  (wrc
   (put-val tx [1 "a" :b 1.0 'c] ["x"])
   (get-val tx [1 "a" :b 1.0 'c])
   ["x"])
  (rc
   (get-val tx [1 "a" :b 1.0])
   nil))

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



(deftest assoc-at+remove-val
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
   (delete-val tx [-1 :people "Sam" :title])
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"})
  (wrc
   (delete-val tx [-1 :people "Sam"])
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"}))

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



(deftest assoc-at+put-overwrite
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
   (put tx [-1 :people "Sam" :title] "Sir")
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"
    :title "Sir"})
  (wrc
   (put tx [-1 :people "Sam"] {:name "Sammy" :profession "Go"})
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


(deftest one-write-assoc-at+remove-val
  (wrc
   (-> tx
       (assoc-at [-1 :people] {"Sam" {:name "Sam"
                                      :title "Mr"}
                               "Sally" {:name "Sally"
                                        :title "Mrs"}})
       (delete-val [-1 :people "Sam" :title])
       (delete-val [-1 :people "Sam"]))
   (get-at tx [-1 :people "Sam"])
   {:name "Sam"}))

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

(deftest one-write-assoc-at+put-overwrite
  (wrc
   (-> tx
       (assoc-at [-1 :people] {"Sam" {:name "Sam"
                                      :title "Mr"}
                               "Sally" {:name "Sally"
                                        :title "Mrs"}})
       (put [-1 :people "Sam" :title] "Sir")
       (put [-1 :people "Sam"] {:name "Sammy" :profession "Go"}))
   (get-at tx [-1])
   {:people {"Sally" {:name "Sally"
                      :title "Mrs"}
             "Sam" {:name "Sammy"
                    :title "Sir"
                    :profession "Go"}}}))
