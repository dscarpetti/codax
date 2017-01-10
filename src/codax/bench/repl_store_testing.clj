(ns codax.bench.repl-store-testing
  (:require
   [codax.store :refer :all]
   [clojure.pprint :refer [pprint]]
   [clojure.string :as str]
   [clojure.test :as t]))

;;;;; Testing

(defn test-insert-and-remove [db node-count]
  (let [keys (range node-count)
        insertions  (shuffle keys)
        deletions (shuffle keys)
        _ (pprint "inserting")
        txn (loop [current-elements (list)
                   remaining-insertions insertions
                   txn (make-transaction db)]
              (assert (=
                   current-elements
                   (doall (map (partial b+get txn) current-elements))))
              (assert (let [all-keys (map first (b+seek txn -100 (inc node-count)))]
                        (if (<= 2 (count all-keys))
                          (apply < all-keys)
                          true)))
              (if (empty? remaining-insertions)
                txn
                (let [el (first remaining-insertions)]
                  (recur (conj current-elements el)
                         (rest remaining-insertions)
                         (b+insert txn el el)))))
        ;;_ (pprint (:dirty-nodes txn))
        ;;_ (draw-tx txn)
        _ (pprint "removing")
        txn (loop [remaining-deletions deletions
                   completed-deletions (list)
                   txn txn]
              (assert (=
                       remaining-deletions)
                      (doall (map (partial b+get txn) remaining-deletions)))
              (assert (let [all-keys (map first (b+seek txn -100 (inc node-count)))]
                        (if (<= 2 (count all-keys))
                          (apply < all-keys)
                          true)))
              (assert (every? nil? (doall (map (partial b+get txn) completed-deletions))))
              (if (empty? remaining-deletions)
                txn
                (let [el (first remaining-deletions)]
                  (recur (rest remaining-deletions)
                         (conj completed-deletions el)
                         (-> txn
                          (b+remove el)
                          (b+remove el))))))]
    (println "final dirty nodes")
    (pprint (:dirty-nodes txn))
    (draw-tx txn)))

(defn commitment-test [path]
  (let [db (open-database path)
        els (range 1000)
        insertions (shuffle els)]
    (with-write-transaction [db tx]
      (reduce (fn [txn el] (b+insert txn el el))
              tx
              insertions))
    (let [old-data @(:data db)
          db (open-database path)
          new-data @(:data db)]
      (assert (= old-data new-data))
      (pprint old-data)
      (pprint new-data))))

(defn stress-database [path]
  ;;(reset! cache-hits 0)
  ;;(reset! cache-misses 0)
  (let [db (open-database path)
        inc-count #(if (number? %)
                     (inc %)
                     1)
        writes (doall (map #(fn [] (with-write-transaction [db tx] (b+insert tx (str %) (str "v" %)))) (range 10000)))
        updates (repeat 10000 #(with-write-transaction [db tx] (b+insert tx "counter" (inc-count (b+get tx "counter")))))
        reads (repeat 10000 #(with-read-transaction [db tx] (b+get tx (str (int (rand 1000)) "x"))))
        seeks nil;(repeat 1000 #(with-read-transaction [db tx] (b+seek tx "0" "z")))
        ops (shuffle (concat writes reads updates seeks))]
    (try
      (dorun (pmap #(%) ops))
      (let [result (with-read-transaction [db tx]
                     (b+seek tx (str (char 0x00)) (str (char 0xff))))]
        ;;(println "cache-size" (count (:cache @(:data db))))
        ;;(println "cache hits" @cache-hits)
        ;;(println "cache misses" @cache-misses)
        (println (count result))
        (println (last result)))
      (finally (close-database path)))))

(defn compaction-test [path]
  (try
    (let [db (open-database path)
          writes-1 (shuffle (map #(fn [] (with-write-transaction [db tx] (b+insert tx % %))) (range 20)))
          check-1 (into [] (map #(vector % %) (range 20)))
          check-2 (into [] (map #(vector % %) (range 40)))
          check-3 (into [] (map #(vector % %) (range 60)))
          check-4 (into [] (map #(vector % %) (range 20 60)))]
      (dorun (map #(%) writes-1))
      (let [run-result (with-read-transaction [db tx] (b+seek tx 0 100))
            _ (println "1. result == check:         " (= run-result check-1))
            db (do (close-database db)
                   (println "1. db closed and reopened")
                   (open-database path))
            ro-result (with-read-transaction [db tx] (b+seek tx 0 100))
            _ (do (println "1. ro-result == check       " (= check-1 ro-result))
                  (compact-database db)
                  (println "1. db compacted"))
            pc-result (with-read-transaction [db tx] (b+seek tx 0 100))
            writes-2 (shuffle (map #(fn [] (with-write-transaction [db tx] (b+insert tx % %))) (range 20 40)))]
        (println "1. pc-result == check" (= check-1 pc-result))
        (dorun (map #(%) writes-2))
        (let [run-result (with-read-transaction [db tx] (b+seek tx 0 100))
              _ (println "2. result == check:          " (= run-result check-2))
              _ (do (compact-database db)
                    (println "2. db compacted"))
              pc-result (with-read-transaction [db tx] (b+seek tx 0 100))
              writes-3 (shuffle (map #(fn [] (with-write-transaction [db tx] (b+insert tx % %))) (range 0 60)))]
          (println "2. pc-result == check       " (= check-2 pc-result))
          (dorun (map #(%) writes-3))
          (let [run-result (with-read-transaction [db tx] (b+seek tx 0 100))
                _ (println "3. result == check:         " (= run-result check-3))
                _ (do (compact-database db)
                      (println "3. db compacted"))
                pc-result (with-read-transaction [db tx] (b+seek tx 0 100))
                writes-4 (shuffle (map #(fn [] (with-write-transaction [db tx] (b+remove tx %))) (range 0 20)))]
            (println "3. pc-result == check:       " (= run-result check-3))
            (dorun (map #(%) writes-4))
            (let [run-result (with-read-transaction [db tx] (b+seek tx 0 100))]
              (println "4. result == check         " (= run-result check-4))
              (let [db (do (close-database db)
                           (println "4. db closed and reopened")
                           (open-database path))
                    ro-result (with-read-transaction [db tx] (b+seek tx 0 100))]
                (println "4. ro-result == check       " (= ro-result check-4))
                (compact-database db)
                (with-write-transaction [db tx] (b+insert tx 99 99))))))))
    (finally (close-database path))))
