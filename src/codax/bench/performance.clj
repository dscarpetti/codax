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

(defn color [c s]
  (str "\033["
       ({:grey 30 :red 31 :green 32 :yellow 33 :blue 34 :magenta 35 :cyan 36 :white 37} c)
       "m" s "\033[0m"))

(defn metric-printer []
  (let [writes (atom 0)]
    (fn [{:keys [compaction-time compaction-number operation-time total-open-time compaction-number writes-since-compaction new-manifest-size
                 old-manifest-size new-nodes-size old-nodes-size] :as data}]
      (let [old-file-size-kb (quot (+ old-manifest-size old-nodes-size) 1024)
            new-file-size-kb (quot (+ new-manifest-size new-nodes-size) 1024)
            ratio (float (/ old-file-size-kb new-file-size-kb))]
        (cl-format *out*
                   (str
                    (color :red "compaction  ~4a  ~a   ~4:dms  ")
                    (color :green "~6:d ops   ~6:d ops/open  ")
                    (color :blue "~4:d wtx/sec  ")
                    (color :magenta "~6:dkB → ~6:dkB (~,2f)~%"))
                   compaction-number
                   (format-nano-interval total-open-time)

                   (long (/ compaction-time 1000000))
                   writes-since-compaction
                   (swap! writes + writes-since-compaction)
                   (long (/ writes-since-compaction (/ operation-time 1000000000)))
                   old-file-size-kb
                   new-file-size-kb
                   ratio)))))


(defn- assoc-user [db user-id]
  (let [timestamp (Math/random)
        user-data (reduce #(assoc %1 (str %2) (Math/random)) {} (take 5 user-id))
        user {:id user-id
              :timestamp timestamp
              :user-data user-data
              }]
    (with-write-transaction [db tx]
      (let [tx (if (get-at tx [:users user-id :id])
                 tx
                 (-> tx
                     (update-at [:metrics :user-counts :all] inc-count)
                     (update-at [:metrics :user-counts :starts-with (str (first user-id))] inc-count)))]
        (assoc-at tx [:users user-id] user)))))

(defn- get-user [db user-id]
  (with-read-transaction [db tx]
    (if (get-at tx [:users user-id])
      1
      0)))


(defn- create-write-operations [database op-count]
  (let [wordlist (take op-count (for [w1 wordlist
                                      w2 wordlist]
                                  (str w1 w2)))]

    (doall (map (fn [word] #(assoc-user database word)) wordlist))))


(defn- create-read-operations [database op-count]
  (let [wordlist (take op-count (for [w1 wordlist
                                      w2 wordlist]
                                  (str w1 w2)))]
    (doall (map (fn [word] #(get-user database word)) wordlist))))

(defn run-benchmark [& {:keys [reads writes] :or {reads 10000 writes 10000}}]
  (binding [codax.store/*monitor-metrics* (metric-printer)]
    (println "Initializing Database...")
    (codax.store/destroy-database "test-databases/BENCH_perf")
    (let [database (open-database! "test-databases/BENCH_perf")]
      (println "Setting Up Performance Benchmark...")
      (try
        (let [write-ops (create-write-operations database reads)
              read-ops (create-read-operations database writes)
              start-time (System/nanoTime)]
          (println "Running Write Performance Benchmark...")
          (println "----")
          (dorun (map #(%) write-ops))
          (println "----")
          (println "Running Read Performance Benchmark...")
          (println "----")
          (let [read-start-time (System/nanoTime)
                read-results (reduce + (doall (pmap #(%) read-ops)))
                read-time (- (System/nanoTime) read-start-time)]
            (cl-format *out*
                       "read ops: ~9:d     op-time: ~5ds     reads/sec: ~5d     users-found: ~5d~%"
                       reads
                       (long (/ read-time 1000000000))
                       (long (/ reads (/ read-time 1000000000)))
                       read-results))
          (println "----")
          (println "Benchmark Complete. Total Time:" (format-nano-interval (- (System/nanoTime) start-time)))
          (println "Counting Individual Records...")
          (cl-format *out* "Total Records: ~:d~%" (with-read-transaction [database tx] (codax.store/b+count-all tx))))
        (finally (close-database! "test-databases/BENCH_perf"))))))
