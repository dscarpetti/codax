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
  (let [user-id (str user-id (int (rand 10000)))
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

(defn- dissoc-user [db user-id]
  (let [user-id (str user-id (int (rand 10000)))
        user-existed (with-read-transaction [db tx] (get-at tx [:users user-id :id]))]
    (if user-existed
      (with-write-transaction [db tx]
        (-> tx
            (update-at [:metrics :user-counts :all] dec-count)
            (update-at [:metrics :user-counts :starts-with (str (first user-id))] dec-count)
            (dissoc-at [:users user-id]))))))

(defn- assoc-long-path [db path]
  (with-write-transaction [db tx]
    (-> tx
        (assoc-at path {:path path
                        :green-eggs-and-ham "I will not eat them"
                        :other (nth path 0)
                        :val (rand)}))))

(defn- create-operation-set [database]
  (let [per-op-count 500005
        wordlist (take per-op-count (cycle wordlist))
        assoc-users (doall (map (fn [word] #(assoc-user database word)) (shuffle wordlist)))
        assoc-long-paths (doall (map (fn [path] #(assoc-long-path database path)) (map #(conj % (str (int (rand 10000))))
                                                                                       (shuffle (partition 10 1 wordlist)))))]
    (cl-format *out* "Prepped ~:d Operations~%" (+ (count assoc-users) (count assoc-long-paths)))
    (shuffle (concat assoc-users assoc-long-paths))))

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
        (println "Getting Depth...")
        (cl-format *out* "Initial Depth: ~:d~%" (with-read-transaction [database tx] (codax.store/b+depth tx)))
        (close-database database)))
    (let [database (open-database "test-databases/BENCH_perf")]
      (println "Setting Up Write Performance Benchmark")
      (try
        (let [opset (create-operation-set database)
              start-time (System/nanoTime)]
          (println "Setup Complete, Running Write Performance Benchmark...")
          (println "----")
          (dorun (map #(%) opset))
          (println "----")
          (println "Benchmark Complete. Total Time:" (format-nano-interval (- (System/nanoTime) start-time)))
          (println "Counting Records...")
          (cl-format *out* "Total Records: ~:d~%" (with-read-transaction [database tx] (codax.store/b+count-all tx)))
          (println "Getting Depth...")
          (cl-format *out* "Final Depth: ~:d~%" (with-read-transaction [database tx] (codax.store/b+depth tx))))
        (finally (close-database "test-databases/BENCH_perf"))))))
