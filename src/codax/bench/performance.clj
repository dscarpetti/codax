(ns codax.bench.performance
  (:require
   [clojure.pprint :refer [pprint cl-format]]
   [clojure.string :as str]
   [codax.backup :refer [make-backup-archiver]]
   [codax.bench.wordlist :refer [wordlist]]
   [codax.core :refer :all]
   [codax.swaps :refer :all]))

(defn format-nano-interval [interval]
  (let [total-seconds (int (/ interval 1000000000))
        seconds (rem total-seconds 60)
        minutes (rem (int (/ total-seconds 60)) 60)
        hours (int (/ total-seconds (* 60 60)))]
    (cl-format nil "~d:~2,'0d:~2,'0d" hours minutes seconds)))

(defn metric-printer [{:keys [compaction-time operation-time total-open-time compaction-number writes-per-compaction]}]
  (cl-format *out*
             "ops: ~9:d     op-time: ~5ds     wtx/sec: ~5d     compaction: ~5dms     total: ~a~%"
             (* compaction-number writes-per-compaction)
             (long (/ operation-time 1000000000))
             (long (/ writes-per-compaction (/ operation-time 1000000000)))
             (long (/ compaction-time 1000000))
             (format-nano-interval total-open-time)))

(defn- assoc-user [db user-id]
  (let [user-id (str user-id (int (rand 1000)))
        timestamp (System/nanoTime)
        test-keys (reduce #(assoc %1 (str %2) (str %2)) {} user-id)
        user {:id user-id
              :timestamp timestamp
              :test-keyset (set (map str user-id))
              :test-keys test-keys}]
    (with-write-transaction [db tx]
      (let [tx (if (get-at tx [:users user-id :id])
                 tx
                 (-> tx
                     (update-at [:metrics :user-counts :all] inc-count)
                     (update-at [:metrics :user-counts :starts-with (str (first user-id))] inc-count)))]
        (assoc-at tx [:users user-id] user)))))

(defn get-user [db user-id]
  (let [user-id (str user-id (int 1000))]
    (with-read-transaction [db tx]
      (if (get-at tx [:users user-id])
        1
        0))))

(defn- assoc-long-path [db path]
  (with-write-transaction [db tx]
    (-> tx
        (assoc-at path {:path path
                        :green-eggs-and-ham "I will not eat them"
                        :other (nth path 0)
                        :val (rand)}))))

(def per-write-op-count 500005)
(def per-read-op-count 1000000)

(defn- create-operation-set [database]
  (let [wordlist (take per-write-op-count (cycle wordlist))
        assoc-users (doall (map (fn [word] #(assoc-user database word)) (shuffle wordlist)))
        assoc-long-paths (doall (map (fn [path] #(assoc-long-path database path)) (map #(conj % (str (int (rand 10000))))
                                                                                       (shuffle (partition 10 1 wordlist)))))
        wordlist (take per-read-op-count (cycle wordlist))
        read-ops (doall (map (fn [word] #(get-user database word)) (shuffle wordlist)))]
    (cl-format *out* "Prepped ~:d Write Operations~%" (+ (count assoc-users) (count assoc-long-paths)))
    (cl-format *out* "Prepped ~:d Read Operations~%" (count read-ops))
    {:write-ops (shuffle (concat assoc-users assoc-long-paths))
     :read-ops read-ops}))

(defn run-benchmark [& [reset]]
  (binding [codax.store/*monitor-metrics* metric-printer]
    (if reset
      (do
        (println "Clearing Database...")
        (codax.store/destroy-database "test-databases/BENCH_perf")
        (println "Database Cleared"))
      (let [database (open-database "test-databases/BENCH_perf")]
        (println "Counting Initial Records...")
        (cl-format *out* "Initial Records: ~:d~%" (with-read-transaction [database tx] (codax.store/b+count-all tx)))
        (println)
        (println "Getting Depth...")
        (cl-format *out* "Initial Depth: ~:d~%" (with-read-transaction [database tx] (codax.store/b+depth tx)))
        (close-database database)))
    (let [database (open-database "test-databases/BENCH_perf")]
      (println)
      (println "Setting Up Performance Benchmark")
      (try
        (let [{:keys [write-ops read-ops]} (create-operation-set database)
              start-time (System/nanoTime)]
          (println "Setup Complete.")
          (println)
          (println "Running Write Performance Benchmark...")
          (println "----")
          (dorun (map #(%) write-ops))
          (println "----")
          (println)
          (println "Running Read Performance Benchmark...")
          (println "----")
          (let [read-start-time (System/nanoTime)
                read-results (reduce + (doall (pmap #(%) read-ops)))
                read-time (- (System/nanoTime) read-start-time)]
            (cl-format *out*
                       "read ops: ~9:d     op-time: ~5ds     reads/sec: ~5d     items-found: ~5d~%"
                       per-read-op-count
                       (long (/ read-time 1000000000))
                       (long (/ per-read-op-count (/ read-time 1000000000)))
                       read-results))
          (println "----")
          (println)
          (println "Benchmark Complete. Total Time:" (format-nano-interval (- (System/nanoTime) start-time)))
          (println)
          (println "Counting Records...")
          (cl-format *out* "Total Records: ~:d~%" (with-read-transaction [database tx] (codax.store/b+count-all tx)))
          (println)
          (println "Getting Depth...")
          (cl-format *out* "Final Depth: ~:d~%" (with-read-transaction [database tx] (codax.store/b+depth tx))))
        (finally (close-database "test-databases/BENCH_perf"))))))
