(ns codax.store
  (:require
   [clojure.core.cache :as cache]
   [clojure.java.io :as io]
   [clojure.pprint :refer [pprint]]
   [clojure.string :as str]
   [taoensso.nippy :as nippy])
  (:import
   [java.io RandomAccessFile FileOutputStream DataOutputStream]
   [java.nio ByteBuffer]
   [java.nio.file Files StandardCopyOption Paths]
   [java.util.concurrent.locks ReentrantReadWriteLock])

  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(def order 40)

(def nippy-options {:compressor nippy/lz4-compressor})

(defn load-node [txn address]
  (let [file (RandomAccessFile. ^String (:filepath (:db txn)) "r")]
    (try
      (.seek file address)
      (let [size (.readLong file)
            data (byte-array size)]
        (.read file data)
        (nippy/thaw data nippy-options))
      (finally
        (.close file)))))

(defn get-node [txn address]
  (if (nil? address)
    {:type :leaf
     :records (sorted-map)}
    (or
     (get (:new-nodes txn) address)
     (let [current-cache (:cache @(:data (:db txn)))]
       (if (cache/has? current-cache address)
         (do
           (swap! (:data (:db txn)) assoc :cache (cache/hit current-cache address))
           (cache/lookup current-cache address))
         (if-let [found-node (load-node txn address)]
           (do
             (swap! (:data (:db txn)) assoc :cache (cache/miss current-cache address found-node))
             found-node)
         (throw (Exception. (str "node not found: " address)))))))))

(defn make-node [txn type records & [old-address]]
  (let [address (promise);(inc (:address txn))
        node {:type type
              :id address
              :records (into (sorted-map) records)}]
    (-> txn
        (assoc :address address)
        (update :new-nodes dissoc old-address)
        (assoc-in [:new-nodes address] node))))

(defn leaf-node? [node]
  (= :leaf (:type node)))

;;; Database & Transactions

(defn get-offset [filepath]
  (let [file (RandomAccessFile. ^String filepath "rw")]
    (try
      (.length file)
      (finally
        (.close file)))))

(defn get-root-address [filepath]
  (io/make-parents filepath)
  (let [file (RandomAccessFile. ^String filepath "rw")]
    (try
      (.seek file (- (.length file) 8))
      (.readLong file)
      (catch Exception e
        nil)
      (finally
        (.close file)))))

(defonce open-databases (atom {}))

(defn close-database [filepath-or-db]
  (let [db (if (string? filepath-or-db)
             (get @open-databases filepath-or-db)
             filepath-or-db)]
    (when db
      (reset! (:data db) nil)
      (swap! open-databases dissoc (:filepath db)))))

(defn destroy-database [filepath-or-db]
  (close-database filepath-or-db)
  (let [filepath (if (string? filepath-or-db)
                   filepath-or-db
                   (:filepath filepath-or-db))]
    (when filepath
      (io/delete-file filepath))))

(defn- create-cache []
  (cache/lru-cache-factory {} :threshold 512))

(defn- create-database [filepath compaction-threshold]
  {:filepath filepath
   :lock-obj (Object.)
   :compaction-lock (ReentrantReadWriteLock. true)
   :compaction-threshold compaction-threshold
   :data (atom {:cache (create-cache)
                :root-id (get-root-address filepath)
                :offset (get-offset filepath)})})

(defn open-database [filepath & {:keys [compaction-threshold] :or {compaction-threshold 1000}}]
  (when (contains? @open-databases filepath)
    (println "replacing open database at" filepath)
    (close-database (get @open-databases filepath)))
  (let [db (create-database filepath compaction-threshold)]
    (swap! open-databases assoc filepath db)
    db))

(defn list-all-databases []
  (println (count (doall (map (fn [[filepath _]] (println filepath)) @open-databases))) "open databases"))

(defn transaction [database]
  (let [data @(:data database)
        root-id (:root-id data)
        txn {:db database
             :address nil
             :new-nodes {};(sorted-map)
             :root-id root-id}]
    txn))


(defn address-node [fulfillments promise type records]
  (let [node {:type type :records records}
        encoded (nippy/freeze node nippy-options)
        block-size (+ 8 (count encoded))
        current-offset (long (+ (:file-offset fulfillments) (:data-offset fulfillments)))]
    (deliver promise current-offset)
    (swap! (:db-data fulfillments) update :cache #(cache/miss % current-offset node))
    (-> fulfillments
        (assoc :last-offset current-offset);;(:data-offset fulfillments))
        (update :data-offset + block-size)
        (update :encoded-objects conj encoded))))

(defn fulfill [fulfillments promise]
  (let [{:keys [type records]} (get (:new-nodes fulfillments) promise)]
    (if (= type :leaf)
      (address-node fulfillments promise :leaf records)
      (let [fulfillments (reduce-kv (fn [ful k p]
                                      (if (get-in ful [:new-nodes p])
                                        (fulfill ful p)
                                        ful))
                                    fulfillments records)
            new-records (into (sorted-map)
                              (reduce-kv (fn [rec k p]
                                           (if (get-in fulfillments [:new-nodes p])
                                             (conj rec [k @p])
                                             (conj rec [k p])))
                                         [] records))]
        (address-node fulfillments promise :internal new-records)))))

(defn update-database! [db filepath file-offset root-location data]
  (let [file (RandomAccessFile. ^String filepath "rwd")]
    (try
      (doto file
        (.seek file-offset)
        (.write (.array ^java.nio.ByteBuffer data)))
      (swap! (:data db) #(assoc %
                          :root-id root-location
                          :offset (.length file)
                          :write-counter (if-let [count (:write-counter %)]
                                           (inc count)
                                           1)))
      nil
      (finally (.close file)))))

(declare compact-database)

(defn commit! [tx]
  (when (:address tx)
    (let [filepath (get-in tx [:db :filepath])
          db-data @(get-in tx [:db :data])
          file-offset (:offset db-data)
          root-id (:root-id tx)
          fulfillment (fulfill {:new-nodes (:new-nodes tx)
                                :file-offset file-offset
                                :data-offset 0
                                :encoded-objects []
                                :db-data (get-in tx [:db :data])}
                               root-id)
          bb (ByteBuffer/allocate (+ 8 (:data-offset fulfillment)))]
      (doseq [ba (:encoded-objects fulfillment)]
        (.putLong bb (long (count ba)))
        (.put bb ^bytes ba))
      (.putLong bb (:last-offset fulfillment))
      (update-database! (:db tx) filepath file-offset (:last-offset fulfillment) bb)
      (when (and (:write-counter db-data)
                 (:compaction-threshold (:db tx))
                 (>= (:write-counter db-data) (:compaction-threshold (:db tx))))
        (compact-database (:db tx))))))

;;;;; Insertion

(defn split-records [txn type records old-address]
  (let [split-pos (int (/ (count records) 2))
        left (take split-pos records)
        right (drop split-pos records)
        split-key (first (first right))
        right (if (= type :internal)
                (assoc-in (vec right) [0 0] nil)
                right)
        left-txn (make-node txn type left old-address)
        left-address (:address left-txn)
        right-txn (make-node left-txn type right)
        right-address (:address right-txn)]
    {:txn right-txn
     :left-address left-address
     :right-address right-address
     :split-key split-key}))

(defn insert-leaf [txn {:keys [records id]} k v]
  (let [new-records (assoc records k v)]
    (if (> order (count new-records))
      (make-node txn :leaf new-records id)
      (split-records txn :leaf new-records id))))


(defn insert-internal [txn {:keys [records id]} k v]
  (let [[child-key child-address] (first (rsubseq records <= k))
        child (get-node txn child-address)
        result (if (leaf-node? child)
                 (insert-leaf txn child k v)
                 (insert-internal txn child k v))
        new-records (if-let [split-key (:split-key result)]
                      (assoc records
                             child-key (:left-address result)
                             split-key (:right-address result))
                      (assoc records child-key (:address result)))
        new-txn (if-let [split-key (:split-key result)]
                  (:txn result)
                  result)]
    (if (>= order (count new-records))
      (make-node new-txn :internal new-records id)
      (split-records new-txn :internal new-records id))))

(defn b+insert [txn k v]
  (let [root-id (:root-id txn)
        root (get-node txn (:root-id txn))
        result (if (leaf-node? root)
                 (insert-leaf txn root k v)
                 (insert-internal txn root k v))]
    (let [new-txn (if-let [split-key (:split-key result)]
                    (make-node (:txn result) :internal [[nil (:left-address result)]
                                                        [split-key (:right-address result)]])
                    result)]
      (-> new-txn
          (update :new-nodes dissoc root-id)
          (assoc  :root-id (:address new-txn))))))

;;;; GET

(defn get-val [txn {:keys [records] :as node} k]
  (cond
    (nil? records) nil
    (leaf-node? node) (get records k)
    :else (recur txn (get-node txn (second (first (rsubseq records <= k)))) k)))

(defn b+get [txn k]
  (let [root (get-node txn (:root-id txn))]
    (get-val txn root k)))


;;;; SEEK

(defn get-sub-seek [records start-key end-key rev]
  (let [matches (subseq records <= end-key)
        index (first (keep-indexed (fn [i [k _]]
                                     ;(println (type k) (type start-key))
                                     (when (and (not (nil? k)) (>= (compare k start-key) 0)) i))
                                   matches))
        sub-matches (if (and index (< 0 index))
                      (drop (dec index) matches)
                      matches)]
    (if rev
      (reverse sub-matches)
      sub-matches)))

(defn seek-vals [results txn {:keys [records] :as node} start-key end-key limit rev]
  (if (and limit (<= limit (count results)))
    (subvec results 0 limit)
    (if (leaf-node? node)
      (reduce conj results (if rev
                             (rsubseq records >= start-key <= end-key)
                             (subseq records >= start-key <= end-key)))
      (reduce (fn [res [_ id]]
                (seek-vals res txn (get-node txn id) start-key end-key limit rev))
              results
              (get-sub-seek records start-key end-key rev)))))

(defn b+seek [txn start end & {:keys [limit reverse]}]
  (let [root (get-node txn (:root-id txn))]
    (seek-vals [] txn root start end limit reverse)))

;;;; REMOVE

(defn remove-leaf [txn {:keys [records id]} k]
  (let [new-records (dissoc records k)]
    (if (< (count new-records) (int (/ order 2)))
      {:combine new-records
       :type :leaf
       :txn txn}
      (make-node txn :leaf new-records id))))

(defn combiner [f txn records child-address child-key child-records [sibling-key sibling-address]]
  (let [sibling (get-node txn sibling-address)
        type (:type sibling)
        sibling-records (:records sibling)
        [smaller-key larger-key smaller-records larger-records] (if (pos? (compare child-key sibling-key)) [sibling-key child-key sibling-records child-records] [child-key sibling-key child-records sibling-records])
        cleaned-records (dissoc records larger-key)
        larger-records (if (= type :leaf)
                         larger-records
                         (assoc larger-records larger-key (get larger-records nil)))
        combined-siblings (merge larger-records smaller-records)
        txn (-> txn
                (update :new-nodes dissoc child-address)
                (update :new-nodes dissoc sibling-address))]
    (f txn cleaned-records type combined-siblings smaller-key)))

(defn distribute-records [txn cleaned-records type combined-siblings smaller-key]
  (let [{:keys [left-address right-address split-key txn]} (split-records txn type combined-siblings nil)]
    {:txn txn
     :records (assoc cleaned-records smaller-key left-address split-key right-address)}))

(defn merge-records [txn cleaned-records type combined-siblings smaller-key]
  (let [new-txn (make-node txn type combined-siblings)]
    {:txn new-txn
     :records (assoc cleaned-records smaller-key (:address new-txn))}))

(defn combine-records [txn records child-address child-key child-records]
  (let [rv (vec records)
        focal-index (first (keep-indexed #(when (= (first %2) child-key) %1) rv))
        left (if (< 0 focal-index) (nth rv (dec focal-index)))
        right (if (< focal-index (dec (count rv))) (nth rv (inc focal-index)))
        left-count (if left (count (:records (get-node txn (second left)))) 0)
        right-count (if right (count (:records (get-node txn (second right)))) 0)
        min-count (int (/ order 2))]
    (cond
      (> right-count min-count) (combiner distribute-records txn records child-address child-key child-records right)
      (> left-count min-count) (combiner distribute-records txn records child-address child-key child-records left)
      (= right-count min-count) (combiner merge-records txn records child-address child-key child-records right)
      (= left-count min-count) (combiner merge-records txn records child-address child-key child-records left))))

(defn remove-internal [txn {:keys [records id]} k]
  (let [[child-key child-address] (first (rsubseq records <= k))
        child (get-node txn child-address)
        result (if (leaf-node? child)
                 (remove-leaf txn child k)
                 (remove-internal txn child k))
        new-records (if-let [child-records (:combine result)]
                      (combine-records (:txn result) records child-address child-key child-records)
                      (assoc records child-key (:address result)))
        new-txn (if (:combine result)
                  (:txn new-records)
                  result)
        new-records (if (:combine result)
                      (:records new-records)
                      new-records)]
    (if (< (count new-records) (int (/ order 2)))
      {:combine new-records
       :type (:type child)
       :origin :internal
       :txn new-txn}
      (make-node new-txn :internal new-records id))))

(defn b+remove [txn k]
  (let [root-id (:root-id txn)
        root (get-node txn (:root-id txn))
        result (if (leaf-node? root)
                 (remove-leaf txn root k)
                 (remove-internal txn root k))]
    (let [final-tx (cond
                     (and (not (= :leaf (:type result))) (:combine result) (= 1 (count (:combine result))))
                     (assoc (:txn result) :address (-> result :combine first second))

                     (and (= :internal (:origin result)) (= :leaf (:type result)) (:combine result))
                     (let [combined (reduce concat (map (fn [[k v]] (vec (:records (get-node (:txn result) v)))) (:combine result)))]
                       (make-node (:txn result) :leaf combined (:address (:txn result))))

                     (and (= :internal (:origin result)) (:combine result))
                     (make-node (:txn result) :internal (:combine result))

                     (:combine result)
                     (make-node (:txn result) (:type result) (:combine result) (:address (:txn result)))

                     :else result)]
      (-> final-tx
          (update :new-nodes dissoc root-id)
          (assoc :root-id (:address final-tx))))))

;;;; Transaction Macros
(defmacro with-read-lock [[database] & body]
  `(let [lock# (:compaction-lock ~database)]
     (.lock (.readLock lock#))
     (try
       (do ~@body)
       (finally (.unlock (.readLock lock#))))))

(defmacro with-write-lock [[database] & body]
  `(let [lock# (:compaction-lock ~database)]
     (.lock (.writeLock lock#))
     (try
       (do ~@body)
       (finally (.unlock (.writeLock lock#))))))

(defmacro with-write-transaction [[database tx-symbol] & body]
  `(let [db# ~database]
     (locking (:lock-obj db#)
       (let [~tx-symbol (transaction db#)]
         (commit! (do ~@body))))))

(defmacro with-read-transaction [[database tx-symbol] & body]
  `(let [db# ~database
         ~tx-symbol (transaction db#)]
     (with-read-lock [db#] ~@body)))


;;;;; Compaction

(defn naive-subset-compaction [origin-filepath compaction-filepath start-key end-key]
  (let [origin-db (open-database origin-filepath)
        compact-db (open-database compaction-filepath)
        data (with-read-transaction [origin-db tx]
               (b+seek tx start-key end-key))]
    (with-write-transaction [compact-db tx]
      (reduce (fn [tx [k v]] (b+insert tx k v)) tx data))))

(defn determine-tree-depth [filepath & [start-address]]
  (let [file (RandomAccessFile. ^String filepath "r")
        read-node (fn [address]
                    (.seek file address)
                    (let [size (.readLong file)
                          data (byte-array size)]
                      (.read file data)
                      (nippy/thaw data nippy-options)))]
    (try
      (loop [address (or start-address (get-root-address filepath))
             depth 0]
        (let [node (read-node address)]
          (if (leaf-node? node)
            (inc depth)
            (recur (second (first (:records node))) (inc depth)))))
      (finally
        (.close file)))))

(defn- clone-leaf-node [^RandomAccessFile origin-file ^DataOutputStream compact-file address]
  (.seek origin-file address)
  (let [size (.readLong origin-file)
        data (byte-array size)
        address (.size compact-file)]
    (.read origin-file data)
    (.writeLong compact-file size)
    (.write compact-file data)
    address))

(defn- load-records-for-compaction [^RandomAccessFile origin-file address]
  (.seek origin-file address)
  (let [size (.readLong origin-file)
        data (byte-array size)]
    (.read origin-file data)
    (:records (nippy/thaw data nippy-options))))

(defn- write-compacted-records [^DataOutputStream compact-file records]
  (let [address (.size compact-file)
        ^bytes encoded-node (nippy/freeze {:type :internal :records records} nippy-options)
        size (long (count encoded-node))]
    (.writeLong compact-file size)
    (.write compact-file encoded-node)
    address))

(defn- compaction-step [origin-file compact-file current-address depth]
  (let [current-records (load-records-for-compaction origin-file current-address)]
    (cond
      (= depth 1) (clone-leaf-node origin-file compact-file current-address)
      (= depth 2)
      (write-compacted-records
       compact-file
       (reduce-kv (fn [new-records k address]
                    (assoc new-records k (clone-leaf-node origin-file compact-file address)))
                  (sorted-map) current-records))
      :else
      (write-compacted-records
       compact-file
       (reduce-kv (fn [new-records k address]
                    (assoc new-records k (compaction-step origin-file compact-file address (dec depth))))
                  (sorted-map) current-records)))))

(defn perform-compaction [origin-path compaction-path]
  (let [root-address (get-root-address origin-path)
        depth (determine-tree-depth origin-path root-address)
        origin-file (RandomAccessFile. ^String origin-path "r")
        compact-file (DataOutputStream. (FileOutputStream. ^String compaction-path))]
    (io/delete-file compaction-path true)
    (try
      (let [root-address (compaction-step origin-file compact-file root-address depth)]
        (.writeLong compact-file root-address)
        (.flush compact-file)
        {:root-id root-address
         :offset (.size compact-file)})
      (finally
        (do
          (.close origin-file)
          (.close compact-file))))))


(defn move-file-atomically [from to]
  (Files/move
   (Paths/get from (make-array String 0))
   (Paths/get to (make-array String 0))
   (into-array [StandardCopyOption/ATOMIC_MOVE])))

(defn compact-database [db]
  (locking (:lock-obj db)
    (let [origin-path (:filepath db)
          temp-path (str origin-path "_TEMP_COMPACTION_FILE")
          archive-path (str origin-path "_archive_" (System/currentTimeMillis) "_" (int (rand 100000)))
          db-data (perform-compaction (:filepath db) temp-path)]
      ;;(move-file-atomically origin-path archive-path)
      (with-write-lock [db] (move-file-atomically temp-path origin-path))
      (reset! (:data db) (assoc db-data :cache (create-cache))))))

(defn compare-dbs [a-path b-path]
  (let [a (open-database a-path)
        b (open-database b-path)
        a-vals (with-read-transaction [a t] (b+seek t (str (char 0x00)) (str (char 0xff))))
        b-vals (with-read-transaction [b t] (b+seek t (str (char 0x00)) (str (char 0xff))))]
    (= a-vals b-vals)))

(defn stress-database []
  (let [db (open-database "data/crash-test-dummy")
        inc-count #(if (number? %)
                     (inc %)
                     1)
        writes (doall (map #(fn [] (with-write-transaction [db tx] (b+insert tx (str %) (str "v" %)))) (range 1000)))
        updates (repeat 1000 #(with-write-transaction [db tx] (b+insert tx "counter" (inc-count (b+get tx "counter")))))
        reads (repeat 500 #(with-read-transaction [db tx] (b+get tx (str (int (rand 1000)) "x"))))
        compacts (repeat 50 #(compact-database db))
        ops (shuffle (concat writes reads compacts updates))]
    (try
      (doall (pmap #(%) ops))
      (let [result (with-read-transaction [db tx]
                     (b+seek tx (str (char 0x00)) (str (char 0xff))))]
        (println (count result))
        (println (last result)))
      (finally (close-database db)))))
