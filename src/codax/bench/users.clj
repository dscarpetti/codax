(ns codax.bench.users
  (:require
   [clojure.string :as str]
   [codax.backup :refer [make-backup-archiver]]
   [codax.bench.wordlist :refer [wordlist]]
   [codax.core :refer :all]
   [codax.swaps :refer :all]))

(defmacro bench
  "Times the execution of forms, discarding their output and returning
a long in nanoseconds."
  ([name & forms]
   `(let [start# (System/nanoTime)
          result# (do ~@forms)]
       [~name result# (- (System/nanoTime) start#)])))

(defn assoc-user [db user-id]
  (bench
   "assoc user"
   (let [;;user-id (str user-id (int (rand 10000)))
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
                      (update-val [:metrics :user-counts :all] inc-count)
                      (update-val [:metrics :user-counts :starts-with (str (first user-id))] inc-count)))]
         (assoc-at tx [:users user-id] user)))
     user-id)))

(defn put-user [db user-id]
  (bench
   "put user"
   (let [;;user-id (str user-id (int (rand 10000)))
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
         (put tx [:users user-id] user)))
     user-id)))

(defn put-val-user [db user-id]
  (bench
   "put val user"
   (let [user-id user-id];;(str user-id (int (rand 10000)))]
     (with-write-transaction [db tx]
       (let [tx (if (get-at tx [:users user-id :id])
                  tx
                  (-> tx
                      (update-val [:metrics :user-counts :all] inc-count)
                      (update-val [:metrics :user-counts :starts-with (str (first user-id))] inc-count)))
             tx (reduce #(put-val %1 [:users user-id :test-keys (str %2)] (str %2)) tx user-id)]
         (-> tx
             (put-val [:users user-id :id] user-id)
             (put-val [:users user-id :timestamp] (System/nanoTime))
             (put-val [:users user-id :test-keyset] (set (map str user-id))))))
     user-id)))

(defn dissoc-user [db user-id]
  (bench
   "delete user"
   (let [user-existed (with-read-transaction [db tx] (get-val tx [:users user-id :id]))]
     (if user-existed
       (with-write-transaction [db tx]
         (-> tx
             (update-at [:metrics :user-counts :all] dec-count)
             (update-at [:metrics :user-counts :starts-with (str (first user-id))] dec-count)
             (dissoc-at [:users user-id])))))))

(defn verify-user-data [db user-id]
  (bench
   "verify user"
   (with-read-transaction [db tx]
     (let [user (get-at tx [:users user-id])]
       (when user
         (if (not (= (set (keys (:test-keys user))) (:test-keyset user)))
           (throw (Exception. (str user-id ": user keys/keyset mismatch " user)))))
       [user-id (if (nil? user) "user not found" "user found")]))))

(defn verify-all-users [db]
  (bench
   "verify all users"
   (with-read-transaction [db tx]
     (let [users (get-at tx [:users])
           all-user-count (get-at tx [:metrics :user-counts :all])]
       ;;(println (count users) all-user-count)
       (if (and all-user-count (not (= (count users) all-user-count)))
         (throw (Exception. (str "count mismatch: " (count users) " " all-user-count "\n"))))))))

(defn compute-times [x]
  (into {}
        (map (fn [[type content]]
         (let [op-count (count content)
               total-time (/ (reduce #(+ %1 (nth %2 2)) 0 content) 1000000000.0)]
           [type {:op-count op-count
                  :total-seconds total-time
                  :ops-per-second (/ op-count total-time)}]))
       x)))

(defn create-user-operation-set [database write-count read-count verification-count]
  (let [wordlist (take (* 4 write-count) wordlist)
        assoc-users (doall (map (fn [word] #(assoc-user database word)) (take write-count (shuffle wordlist))))
        put-users (doall (map (fn [word] #(put-user database word)) (take write-count (shuffle wordlist))))
        put-val-users (doall (map (fn [word] #(put-val-user database word)) (take write-count (shuffle wordlist))))
        dissoc-users (doall (map (fn [word] #(dissoc-user database word)) (take write-count (shuffle wordlist))))
        user-verification (repeatedly read-count #(fn [] (verify-user-data database (first (shuffle wordlist)))))
        all-users-verification (repeatedly verification-count #(fn [] (verify-all-users database)))]
    (shuffle (concat assoc-users put-users put-val-users user-verification dissoc-users all-users-verification))))
                     ;;[#(do (close-database database) (codax.store/compact-database database))]))))

(defn run-user-test [& {:keys [no-cache writes reads verifications] :or {writes 1500 reads 7500 verifications 0}}]
  (let [backup-archiver (make-backup-archiver :bzip2 #(do (println) (println %) (println)))
        database (open-database "test-databases/BENCH_user" :backup-fn backup-archiver)]
    (try
      (let [opset-1 (create-user-operation-set database writes reads verifications)
            opset-2 (create-user-operation-set database writes reads verifications)
            _ (println "Setup complete. Starting test.")
            start-time (System/nanoTime)
            results-1 (doall (pmap #(%) opset-1))
            results-2 (doall (pmap #(%) opset-2))
            stop-time (System/nanoTime)
            results (concat results-1 results-2)
            results (group-by first results)
            verification-results (group-by (fn [x] (second (second x))) (get results "verify user"))
            results (merge (dissoc results "verify user") verification-results)
            total-time (compute-times results)]
        (clojure.pprint/pprint total-time)
        (println "Verifying all users...")
        (time (verify-all-users database))
        {:clock-time (/ (- stop-time start-time) 1000000000.0)
         :stats total-time
         :cache (not no-cache)})
      (finally (close-database "test-databases/BENCH_user")))))
