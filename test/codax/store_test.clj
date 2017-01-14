(ns codax.store-test
  (:require
   [codax.store :refer :all]
   [clojure.java.io :as io]
   [clojure.pprint :refer [pprint]]
   [clojure.string :as str]
   [clojure.test :refer :all])
  (:import
   [java.io RandomAccessFile]))

(def ^:dynamic *testing-database* nil)

(defn store-setup-and-teardown [f]
  (binding [*testing-database* (open-database "test-databases/store")]
    (f))
  (destroy-database "test-databases/store"))

(use-fixtures :each store-setup-and-teardown)

(deftest attempt-reopen
  (is (thrown-with-msg?
       Exception
       #"Database Already Open"
       (open-database (:path *testing-database*)))))

(deftest open-non-folder
  (spit "test-databases/non-folder" "content")
  (try
    (open-database "test-databases/non-folder")
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [cause] :as data} (ex-data e)]
        (println data)
        (is (= cause :not-a-directory))
        (is (= (.getMessage e) "Invalid Database"))))
    (finally (io/delete-file "test-databases/non-folder"))))

(defmacro with-simulated-manifest [[path type-long version-int order-int] & body]
  `(let [path# ~path]
     (io/make-parents (str path# "/manifest"))
     (let [file# (RandomAccessFile. (str path# "/manifest") "rws")]
       (do
         (.writeLong file# (long ~type-long))
         (.writeInt file# (int ~version-int))
         (.writeInt file# (int ~order-int)))
         (.close file#))
       (try
         (do ~@body)
         (finally
           (io/delete-file (str path# "/manifest"))
           (io/delete-file path#)))))

(deftest open-invalid-type
  (with-simulated-manifest ["test-databases/bad-type-flag"
                            (inc codax.store/file-type-tag)
                            codax.store/file-version-tag
                            codax.store/order]
    (try
      (open-database "test-databases/bad-type-flag")
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [cause] :as data} (ex-data e)]
          (println data)
          (is (= cause :file-type-mismatch))
          (is (= (.getMessage e) "Invalid Database")))))))

(deftest open-invalid-version
  (with-simulated-manifest ["test-databases/bad-version-flag"
                            codax.store/file-type-tag
                            (inc codax.store/file-version-tag)
                            codax.store/order]
    (try
      (open-database "test-databases/bad-version-flag")
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [cause] :as data} (ex-data e)]
          (println data)
          (is (= cause :version-mismatch))
          (is (= (.getMessage e) "Incompatible Database")))))))

(deftest open-invalid-order
  (with-simulated-manifest ["test-databases/bad-order-param"
                            codax.store/file-type-tag
                            codax.store/file-version-tag
                            (inc codax.store/order)]
    (try
      (open-database "test-databases/bad-order-param")
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [cause] :as data} (ex-data e)]
          (println data)
          (is (= cause :order-mismatch))
          (is (= (.getMessage e) "Incompatible Database")))))))


(deftest read-tx-on-closed-database
  (try
    (do
      (close-database *testing-database*)
      (with-read-transaction [*testing-database* tx]))

    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [cause] :as data} (ex-data e)]
        (println data)
        (is (= cause :attempted-transaction))
        (is (= (.getMessage e) "Database Closed"))))))

(deftest write-tx-on-closed-database
  (try
    (do
      (close-database *testing-database*)
      (with-write-transaction [*testing-database* tx]))

    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [cause] :as data} (ex-data e)]
        (println data)
        (is (= cause :attempted-transaction))
        (is (= (.getMessage e) "Database Closed"))))))


(deftest compaction-on-closed-database
  (try
    (do
      (close-database *testing-database*)
      (compact-database *testing-database*))
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [cause] :as data} (ex-data e)]
        (println data)
        (is (= cause :attempted-compaction))
        (is (= (.getMessage e) "Database Closed"))))))

(defn insert-and-remove [db node-count]
  (let [keys (range node-count)
        insertions  (shuffle keys)
        deletions (shuffle keys)
        txn (loop [current-elements (list)
                   remaining-insertions insertions
                   txn (make-transaction db)]
              (is (=
                   current-elements
                   (doall (map (partial b+get txn) current-elements))))
              (is (let [all-keys (map first (b+seek txn -100 (inc node-count)))]
                    (if (<= 2 (count all-keys))
                      (apply < all-keys)
                      true)))
              (if (empty? remaining-insertions)
                txn
                (let [el (first remaining-insertions)]
                  (recur (conj current-elements el)
                         (rest remaining-insertions)
                         (b+insert txn el el)))))
        txn (loop [remaining-deletions deletions
                   completed-deletions (list)
                   txn txn]
              (is (=
                   remaining-deletions
                   (doall (map (partial b+get txn) remaining-deletions))))
              (is (let [all-keys (map first (b+seek txn -100 (inc node-count)))]
                    (if (<= 2 (count all-keys))
                      (apply < all-keys)
                      true)))
              (is (every? nil? (doall (map (partial b+get txn) completed-deletions))))
              (if (empty? remaining-deletions)
                txn
                (let [el (first remaining-deletions)]
                  (recur (rest remaining-deletions)
                         (conj completed-deletions el)
                         (-> txn
                             (b+remove el)
                             (b+remove el))))))]
    (is (= (:root-id txn) 1))))

(deftest test-insert-and-remove
  (insert-and-remove *testing-database* 1000))

(deftest test-insert-and-remove-sub-order
  (insert-and-remove *testing-database* (dec order)))

(deftest commit-reopen-test
  (let [db *testing-database*
        els (range 1000)
        insertions (shuffle els)]
    (with-write-transaction [db tx]
      (reduce (fn [txn el] (b+insert txn el el))
              tx
              insertions))
    (let [old-data @(:data db)
          _ (close-database (:path db))
          db (open-database (:path db))
          new-data @(:data db)]
      (is (= (select-keys old-data [:root-id :id-counter :manifest :nodes-offset])
             (select-keys new-data [:root-id :id-counter :manifest :nodes-offset]))))))



(defn stress-database [db op-count]
  (let [inc-count #(if (number? %)
                     (inc %)
                     1)
        read-op-count (* 10 op-count)
        writes (doall (map #(fn [] (with-write-transaction [db tx] (b+insert tx (str %) (str "v" %)))) (range op-count)))
        updates (repeat op-count #(with-write-transaction [db tx] (b+insert tx "counter" (inc-count (b+get tx "counter")))))
        reads (repeat read-op-count #(with-read-transaction [db tx] (b+get tx (str (int (rand op-count))))))
        ops (shuffle (concat writes reads updates))
        start-time (System/currentTimeMillis)]
    (dorun (pmap #(%) ops))
    (let [seconds (/ (- (System/currentTimeMillis) start-time) 1000)]
      (println "Took ~"
               (int seconds)
               "seconds to interleave "
               (* op-count 2)
               " write transactions (~"
               (int (/ (* op-count 2) seconds))
               " wtx/sec ) with "
               read-op-count
               " read transactions (~"
               (int (/ read-op-count seconds ))
               " rtx/sec ) "))
    (let [result (with-read-transaction [db tx]
                   (b+seek tx (str (char 0x00)) (str (char 0xff))))
          partitioned-result (partition 2 1 result)]
      (is (every? (fn [[a b]] (neg? (compare a b))) partitioned-result))
      (is (= (count result) (inc op-count))) ;; inc op-count because all updates occur on the same key
      (is (= (last result) ["counter" op-count])))))

(deftest stress-small
  (stress-database *testing-database* 1000))

(deftest stress-large
  (stress-database *testing-database* 10000))

(defn compaction-test [db]
  (let [path (:path db)
        writes-1 (shuffle (map #(fn [] (with-write-transaction [db tx] (b+insert tx % %))) (range 20)))
        check-1 (into [] (map #(vector % %) (range 20)))
        check-2 (into [] (map #(vector % %) (range 40)))
        check-3 (into [] (map #(vector % %) (range 60)))
        check-4 (into [] (map #(vector % %) (range 20 60)))]
    (dorun (map #(%) writes-1))
    (let [run-result (with-read-transaction [db tx] (b+seek tx 0 100))
          _ (is (= run-result check-1))
          db (do (close-database db)
                 (open-database path))
          ro-result (with-read-transaction [db tx] (b+seek tx 0 100))
          _ (do (is (= check-1 ro-result))
                (compact-database db))
          pc-result (with-read-transaction [db tx] (b+seek tx 0 100))
          writes-2 (shuffle (map #(fn [] (with-write-transaction [db tx] (b+insert tx % %))) (range 20 40)))]
      (is (= check-1 pc-result))
      (dorun (map #(%) writes-2))
      (let [run-result (with-read-transaction [db tx] (b+seek tx 0 100))
            _ (do (is (= run-result check-2))
                  (compact-database db))
            pc-result (with-read-transaction [db tx] (b+seek tx 0 100))
            writes-3 (shuffle (map #(fn [] (with-write-transaction [db tx] (b+insert tx % %))) (range 0 60)))]
        (is (= check-2 pc-result))
        (dorun (map #(%) writes-3))
        (let [run-result (with-read-transaction [db tx] (b+seek tx 0 100))
              _ (do (is (= run-result check-3))
                    (compact-database db))
              pc-result (with-read-transaction [db tx] (b+seek tx 0 100))
              writes-4 (shuffle (map #(fn [] (with-write-transaction [db tx] (b+remove tx %))) (range 0 20)))]
          (is (= run-result check-3))
          (dorun (map #(%) writes-4))
          (let [run-result (with-read-transaction [db tx] (b+seek tx 0 100))]
            (is (= run-result check-4))
            (let [db (do (close-database db)
                         (open-database path))
                  ro-result (with-read-transaction [db tx] (b+seek tx 0 100))]
              (is (= ro-result check-4))
              (compact-database db)
              (with-write-transaction [db tx] (b+insert tx 99 99))
              (is (= 99 (with-read-transaction [db tx] (b+get tx 99))))
              (is (= 42 (with-read-transaction [db tx] (b+get tx 42)))))))))))

(deftest test-compaction
  (compaction-test *testing-database*))
