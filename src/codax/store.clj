(ns codax.store
  (:require
   [clj-time.core :as time]
   [clj-time.format :as format-time]
   [clojure.core.cache :as cache]
   [clojure.java.io :as io]
   [taoensso.nippy :as nippy])
  (:import
   [java.io RandomAccessFile]
   [java.nio ByteBuffer]
   [java.nio.channels FileChannel]
   [java.nio.file Files Paths StandardCopyOption StandardOpenOption]
   [java.util.concurrent.locks ReentrantReadWriteLock ReentrantLock]))

;;;;; Settings

(def order 256)
(def cache-threshold 64)
(def nippy-options {:compressor nippy/lz4-compressor})
(def ^:dynamic *monitor-metrics* nil)

;;;;; Databases

(def file-type-tag (long 14404350))
(def file-version-tag (int 1))
(defonce open-databases (atom {}))

(defn- to-path [str]
  (Paths/get str (make-array String 0)))

(defn- to-canonical-path-string [str]
  (.getCanonicalPath (io/as-file str)))

(defn- read-manifest-buffer [^ByteBuffer buf]
  (.position buf 16)
  (loop [root-id 1
         id-counter 1
         manifest (transient {})]
    (if (.hasRemaining buf)
      (let [id (.getLong buf)
            new-id-counter (max id id-counter)]
        (if (= 0 id)
          (recur (.getLong buf) new-id-counter manifest)
          (recur root-id new-id-counter (assoc! manifest id (.getLong buf)))))
      {:root-id root-id
       :id-counter id-counter
       :manifest (persistent! manifest)})))

(defn- load-manifest [path]
  (let [dir (io/as-file path)]
    (when (and (.exists dir) (not (.isDirectory dir)))
      (throw (ex-info "Invalid Database" {:cause :not-a-directory
                                          :message "Valid databases are directories."
                                          :path path})))
    (when (not (.exists dir))
      (io/make-parents (str path "/files")))
    (let [manifest-file (RandomAccessFile. (str path "/manifest") "rw")]
      (try
        ;; check or write file-type-tag and file-version-tag
        (if (pos? (.length manifest-file))
          (let [file-type-long (.readLong manifest-file)
                file-version-int (.readInt manifest-file)
                order-int (.readInt manifest-file)]
            (when (not (= file-type-long file-type-tag))
              (throw (ex-info "Invalid Database" {:cause :file-type-mismatch
                                                  :message "The manifest file is from another program or has been corrupted."})))
            (when (not (= file-version-int file-version-tag))
              (throw (ex-info "Incompatible Database" {:cause :version-mismatch
                                                       :message "This database was created with an incompatible version."
                                                       :system-version file-version-tag
                                                       :file-version file-version-int})))
            (when (not (= order-int order))
              (throw (ex-info "Incompatible Database" {:cause :order-mismatch
                                                       :message "This database was created with a different node size."
                                                       :system-order order
                                                       :file-order order-int}))))
          (do
            (.writeLong manifest-file file-type-tag)
            (.writeInt manifest-file file-version-tag)
            (.writeInt manifest-file order)))
        (finally (.close manifest-file)))
      (read-manifest-buffer (ByteBuffer/wrap (Files/readAllBytes (to-path (str path "/manifest"))))))))

(defn- load-nodes-offset [path]
  (let [node-file (RandomAccessFile. (str path "/nodes") "rw")]
    (try
      (.length node-file)
      (finally (.close node-file)))))

(defn- open-file-handles [data path]
  (assoc data
         :manifest-channel (FileChannel/open (to-path (str path "/manifest")) (into-array [StandardOpenOption/APPEND
                                                                                           StandardOpenOption/SYNC]))
         :nodes-channel (FileChannel/open (to-path (str path "/nodes")) (into-array [StandardOpenOption/APPEND
                                                                                     StandardOpenOption/SYNC]))
         :file-reader (RandomAccessFile. (str path "/nodes") "r")))

(defn- close-file-handles [db]
  (let [data @(:data db)]
    (.close ^FileChannel (:manifest-channel data))
    (.close ^FileChannel (:nodes-channel data))
    (.close ^RandomAccessFile (:file-reader data))))

(defn- initialize-database-data! [{:keys [path data] :as database} manifest nodes-offset]
  (swap! data
         #(-> %
              (open-file-handles path)
              (assoc :cache (cache/lru-cache-factory {} :threshold cache-threshold)
                     :manifest manifest
                     :writes-since-compaction 0
                     :nodes-offset nodes-offset)))
  database)

;;; Locks

(defmacro with-read-lock [[database] & body]
  `(let [^ReentrantReadWriteLock lock# (:compaction-lock ~database)]
     (.lock (.readLock lock#))
     (try
       (do ~@body)
       (finally (.unlock (.readLock lock#))))))

(defmacro with-compaction-lock [[database] & body]
  `(let [^ReentrantReadWriteLock lock# (:compaction-lock ~database)]
     (.lock (.writeLock lock#))
     (try
       (do ~@body)
       (finally (.unlock (.writeLock lock#))))))

(defmacro with-write-lock [[database increment-counter] & body]
  (if increment-counter
    `(let [[^ReentrantLock lock# counter#] (:write-lock ~database)]
       (.lock lock#)
       (try
         (do
           ~@body
           (swap! counter# inc))
         (finally (.unlock lock#))))
    `(let [[^ReentrantLock lock#] (:write-lock ~database)]
       (.lock lock#)
       (try
         (do ~@body)
         (finally (.unlock lock#))))))

;;; Compaction

(defn- compact-manifest [path manifest root-id]
  (let [compact-manifest-buffer (ByteBuffer/allocate (* 16 (+ 2 (count manifest))))
        compact-manifest-channel (FileChannel/open (to-path (str path "/manifest_COMPACT")) (into-array [StandardOpenOption/CREATE_NEW
                                                                                                         StandardOpenOption/WRITE
                                                                                                         StandardOpenOption/SYNC]))]
    (doto compact-manifest-buffer
      (.putLong file-type-tag)
      (.putInt file-version-tag)
      (.putInt order))
    (loop [remaining-manifest manifest]
      (when-let [[id address] (first remaining-manifest)]
        (doto compact-manifest-buffer
          (.putLong id)
          (.putLong address))
        (recur (rest remaining-manifest))))
    (doto compact-manifest-buffer
      (.putLong 0)
      (.putLong root-id)
      (.flip))
    (doto compact-manifest-channel
      (.write compact-manifest-buffer)
      (.close))))

(defn- compact-nodes [path manifest]
  (let [nodes-channel (FileChannel/open (to-path (str path "/nodes")) (into-array [StandardOpenOption/READ]))
        compact-nodes-channel (FileChannel/open (to-path (str path "/nodes_COMPACT")) (into-array [StandardOpenOption/CREATE_NEW
                                                                                                   StandardOpenOption/APPEND]))]
    (loop [address 0
           new-manifest (transient {})
           remaining-manifest manifest]
      (if-let [[id original-address] (first remaining-manifest)]
        (do
          (.position nodes-channel ^long original-address)
          (let [size-buf (ByteBuffer/allocate 8)
                _ (.read nodes-channel size-buf)
                size (.getLong ^ByteBuffer (.flip size-buf))]
            (.write compact-nodes-channel ^ByteBuffer (.flip size-buf))
            (.transferFrom compact-nodes-channel nodes-channel (+ 8 address) size)
            (recur (+ 8 size address)
                   (assoc! new-manifest id address)
                   (rest remaining-manifest))))
        (do
          (.write compact-nodes-channel (ByteBuffer/allocate 8)) ; 8 empty bytes indicate the end of a commit
          (.force compact-nodes-channel true)
          (.close compact-nodes-channel)
          (.close nodes-channel)
          {:new-manifest (persistent! new-manifest)
           :new-nodes-offset (+ 8 address)})))))

(defn- move-file-atomically [from to]
    (Files/move (to-path from) (to-path to) (into-array [StandardCopyOption/ATOMIC_MOVE])))

(def ts-formatter (format-time/formatters :basic-date-time-no-ms))

(defn- relocate-compact-files [path backup-fn]
  (if backup-fn
    (let [ts-suffix (str "_" (format-time/unparse ts-formatter (time/now)) "_" (System/nanoTime))]
      (move-file-atomically (str path "/nodes") (str path "/nodes" ts-suffix))
      (move-file-atomically (str path "/manifest") (str path "/manifest" ts-suffix))
      (future (backup-fn {:dir path
                          :suffix ts-suffix
                          :file-names [(str "nodes" ts-suffix) (str "manifest" ts-suffix)]})))

    (do
      (move-file-atomically (str path "/nodes") (str path "/nodes_ARCHIVE"))
      (move-file-atomically (str path "/manifest") (str path "/manifest_ARCHIVE"))))
  (move-file-atomically (str path "/nodes_COMPACT") (str path "/nodes"))
  (move-file-atomically (str path "/manifest_COMPACT") (str path "/manifest")))

(defn update-metrics! [db start-compaction-time end-compaction-time writes-since-compaction old-manifest-size old-nodes-size]
  (when *monitor-metrics*
    (let [metrics @(:metrics db)
          new-nodes-size (.size (-> db :data deref :nodes-channel))
          last-compacted-at (or (:compacted-at metrics) (:opened-at metrics))
          compaction-number (inc (:compactions metrics))]
      (swap! (:metrics db) assoc :compacted-at end-compaction-time :compactions compaction-number)
      (when *monitor-metrics* (*monitor-metrics* {:new-manifest-size (* 16 (+ 2 (-> db :data deref :manifest count)))
                                                  :old-manifest-size old-manifest-size
                                                  :new-nodes-size new-nodes-size
                                                  :old-nodes-size old-nodes-size
                                                  :compaction-time (- end-compaction-time start-compaction-time)
                                                  :operation-time (- start-compaction-time last-compacted-at)
                                                  :total-open-time (- end-compaction-time (:opened-at metrics))
                                                  :compaction-number compaction-number
                                                  :writes-since-compaction writes-since-compaction})))))

(defn compact-database [db]
  (with-write-lock [db false]
    (let [start-compaction-time (System/nanoTime)
          path (:path db)
          {:keys [manifest root-id is-closed writes-since-compaction ^FileChannel manifest-channel ^FileChannel nodes-channel]} @(:data db)
          _ (when is-closed (throw (ex-info "Database Closed" {:cause :attempted-compaction
                                                               :message "The database object has been invalidated."
                                                               :path path})))
          old-manifest-size (.size manifest-channel)
          old-nodes-size (.size nodes-channel)
          {:keys [new-manifest new-nodes-offset]} (compact-nodes path manifest)]
      (compact-manifest path new-manifest root-id)
      (with-compaction-lock [db]
        (close-file-handles db)
        (relocate-compact-files path (:backup-fn db))
        (initialize-database-data! db new-manifest new-nodes-offset)
        (update-metrics! db start-compaction-time (System/nanoTime) writes-since-compaction old-manifest-size old-nodes-size))
      true)))

;;; Open/Close/Destroy

(defonce connection-lock (Object.))

(defn close-database [path-or-db]
  (locking connection-lock
    (let [path (to-canonical-path-string
                (if (string? path-or-db)
                  path-or-db
                  (:path path-or-db)))]
      (when-let [open-db (@open-databases path)]
        (with-write-lock [open-db false]
          (with-compaction-lock [open-db]
            (when (not (:is-closed @(:data open-db)))
              (close-file-handles open-db)
              (reset! (:data open-db) {:is-closed true})
              (swap! open-databases dissoc path open-db))))
        true))))

(defn close-all-databases []
  (locking connection-lock
    (doseq [[path db] @open-databases]
      (close-database path))))

(defn destroy-database
  "Removes database files and generic archive files.
  If there is nothing else in the database directory, it is also removed."
  [path-or-db]
  (locking connection-lock
    (close-database path-or-db)
    (let [path (to-canonical-path-string
                (if (string? path-or-db)
                  path-or-db
                  (:path path-or-db)))
          directory (io/as-file path)
          nodes-file (io/as-file (str path "/nodes"))
          manifest-file (io/as-file (str path "/manifest"))
          nodes-archive-file (io/as-file (str path "/nodes_ARCHIVE"))
          manifest-archive-file (io/as-file (str path "/manifest_ARCHIVE"))]
      (when (.exists nodes-file) (io/delete-file nodes-file))
      (when (.exists manifest-file) (io/delete-file manifest-file))
      (when (.exists nodes-archive-file) (io/delete-file nodes-archive-file))
      (when (.exists manifest-archive-file) (io/delete-file manifest-archive-file))
      (when (and (.exists directory) (.isDirectory directory) (zero? (count (.list directory))))
        (io/delete-file directory)))))

(defn- existing-connection [path backup-fn]
  (let [path (to-canonical-path-string path)
        db (@open-databases path)]
    (when db
      (if (= backup-fn (:backup-fn db))
        db
        (throw (ex-info "Mismatched Backup Functions" {:cause :backup-mismatch
                                                       :message "Attempted database access with a different backup function"
                                                       :path path}))))))

(defn- open-new-connection [path backup-fn]
  (let [path (to-canonical-path-string path)
        {:keys [root-id id-counter manifest]} (load-manifest path)
        nodes-offset (load-nodes-offset path)
        db {:codax.store/is-database true
            :path path
            :backup-fn backup-fn
            :write-lock [(ReentrantLock. true) (atom 0)]
            :compaction-lock (ReentrantReadWriteLock. true)
            :metrics (atom {:opened-at (System/nanoTime)
                            :compactions 0})
            :data (atom {:root-id root-id
                         :id-counter id-counter})}]
    (initialize-database-data! db manifest nodes-offset)
    (swap! open-databases assoc path db)
    db))

(defn open-database
  "Establishes a new, or fetches the existing, connection to the database at the supplied path.

  If the connection already exists, `backup-fn` must match the existing `backup-fn`.
  To change the `backup-fn` the database must first be closed."
  [path & [backup-fn]]
  (locking connection-lock
    (or
     (existing-connection path backup-fn)
     (open-new-connection path backup-fn))))

(defn is-open? [path-or-db]
  (let [db (if (string? path-or-db)
             (get @open-databases (to-canonical-path-string path-or-db))
             path-or-db)]
    (if db
      (not (:is-closed @(:data db)))
      false)))


;;;;; Transactions

(defn- update-database! [{:keys [db root-id id-counter manifest]} nodes-offset manifest-delta dirty-ids nodes-by-address]
  (swap! (:data db)
         (fn [data]
           (let [updated-cache (reduce-kv (fn [c address node]
                                            (let [old-address (manifest (:id node))]
                                              (-> c
                                                  (cache/evict old-address)
                                                  (cache/miss address node))))
                                          (:cache data)
                                          nodes-by-address)]
             (-> data
                 (assoc :root-id root-id
                        :cache updated-cache
                        :id-counter id-counter
                        :writes-since-compaction (inc (:writes-since-compaction data))
                        :nodes-offset nodes-offset)
                 (update :manifest #(apply dissoc % dirty-ids))
                 (update :manifest merge manifest-delta)))))
  (let [^FileChannel manifest-channel (-> db :data deref :manifest-channel)
        size-of-file-manifest (/ (.size manifest-channel) 16)
        auto-compaction-factor (/ (Math/pow size-of-file-manifest 1.2) (+ 2 (count manifest)))]
    (when (> auto-compaction-factor 25)
      (compact-database db))))

(defn- save-buffers! [db ^ByteBuffer manifest-buffer ^ByteBuffer nodes-buffer]
  (let [data @(:data db)
        ^FileChannel manifest-channel (:manifest-channel data)
        ^FileChannel nodes-channel (:nodes-channel data)]
    (.write manifest-channel ^ByteBuffer (.flip manifest-buffer))
    (.write nodes-channel ^ByteBuffer (.flip nodes-buffer))))

(defn commit! [txn]
  (let [dirty-ids (keys (:dirty-nodes txn))
        dirty-nodes (remove (comp nil? second) (:dirty-nodes txn))
        manifest-buffer (ByteBuffer/allocate (* 16 (inc (count dirty-nodes))))]
    (loop [remaining-nodes dirty-nodes
           address (:nodes-offset txn)
           manifest-delta {}
           node-buffers []
           nodes-by-address {}
           total-length 0]
      (if (empty? remaining-nodes)
        (let [all-nodes-buffer (ByteBuffer/allocate (+ 8 total-length))]
          (doseq [^ByteBuffer b node-buffers]
            (.put all-nodes-buffer (.array b)))
          (.putLong all-nodes-buffer (long 0)) ;; 8 empty bytes indicate the end of a commit
          (.putLong manifest-buffer (long 0)) ;; 8 empty bytes indicate the end of a commit
          (.putLong manifest-buffer (long (:root-id txn)))
          (save-buffers! (:db txn) manifest-buffer all-nodes-buffer)
          (update-database! txn (+ 8 address) manifest-delta dirty-ids nodes-by-address))
        (let [[id node] (first remaining-nodes)
              ^bytes encoded-value (nippy/freeze node nippy-options)
              size (count encoded-value)
              buf (ByteBuffer/allocate (+ 8 size))]
          (.putLong buf (long size))
          (.put buf encoded-value)
          (.putLong manifest-buffer (long id))
          (.putLong manifest-buffer (long address))
          (recur (rest remaining-nodes)
                 (+ 8 address size)
                 (assoc manifest-delta id address)
                 (conj node-buffers buf)
                 (assoc nodes-by-address address node)
                 (+ 8 size total-length))))))
  nil)


(defn make-transaction [database]
  (let [{:keys [manifest root-id id-counter nodes-offset is-closed]} @(:data database)]
    (when is-closed (throw (ex-info "Database Closed" {:cause :attempted-transaction
                                                       :message "The database object has been invalidated."
                                                       :path (:path database)})))
    {:codax.store/is-transaction true
     :db database
     :root-id root-id
     :id-counter id-counter
     :nodes-offset nodes-offset
     :manifest manifest
     :dirty-nodes {}}))

;;; Macros

(defn assert-db [db & [msg]]
  (when (not (and (map? db) (:codax.store/is-database db)))
    (throw (ex-info "Invalid Database" {:cause :invalid-database
                                        :message (or msg "expected a database")}))))

(defn assert-txn [txn & [msg]]
  (when (not (and (map? txn) (:codax.store/is-transaction txn)))
    (throw (ex-info "Invalid Transaction" {:cause :invalid-transaction
                                           :message (or msg "expected a transaction")
                                           :got txn}))))

(defmacro with-write-transaction [[database tx-symbol] & body]
  `(let [db# ~database]
     (assert-db db#)
     (with-write-lock [db# true]
       (let [~tx-symbol (make-transaction db#)
             result-tx# (do ~@body)]
         (assert-txn result-tx#  "the body of `with-write-transaction` must evaluate to a transaction")
         (commit! result-tx#)))
     nil))

(defmacro with-read-transaction [[database tx-symbol] & body]
  `(let [db# ~database]
     (assert-db db#)
     (with-read-lock [db#]
       (let [~tx-symbol (make-transaction db#)]
         ~@body))))

(defn throw-upgrade-restart [tx]
  (throw (ex-info "Upgrading Transaction" {:cause :upgrade-restart-required
                                           :message "upgrading the transaction requires restarting it"
                                           :db-path (-> tx :db :path)
                                           :upgrade-transaction true})))

(defn maybe-upgrade-txn [{:keys [upgrade-nonce] :as tx}]
  (if upgrade-nonce
    (let [[^ReentrantLock lock counter] (:write-lock (:db tx))
          acquired-lock (.tryLock lock)]
      (if acquired-lock
        (when (not= @counter upgrade-nonce)
          (.unlock lock)
          (throw-upgrade-restart tx))
        (throw-upgrade-restart tx)))
    tx))

(defmacro with-upgradable-transaction [[database tx-symbol throw-on-restart] & body]
  `(let [db# ~database]
     (assert-db db#)
     (let [[^ReentrantLock lock# counter#] (:write-lock db#)]
       (when (.isHeldByCurrentThread lock#)
         (throw (ex-info "Nested Upgradable Transaction" {:cause :nested-upgradable-transaction
                                                          :message "upgradable transactions cannot be started within another transaction"})))
       (try
         (with-read-lock [db#]
           (let [~tx-symbol (assoc (make-transaction db#) :upgrade-nonce @counter#)
                 result-tx# (do ~@body)]
             (assert-txn result-tx#  "the body of `with-upgradable-transaction` must evaluate to a transaction")
             (when (.isHeldByCurrentThread lock#)
               (commit! result-tx#)
               (swap! counter# inc))))
         (catch clojure.lang.ExceptionInfo e#
           (if (and (:upgrade-transaction (ex-data e#)) (not ~throw-on-restart))
             (with-write-lock [db# true]
               (let [~tx-symbol (make-transaction db#)
                     result-tx# (do ~@body)]
                 (assert-txn result-tx#  "the body of `with-upgradable-transaction` must evaluate to a transaction")
                 (commit! result-tx#)))
             (throw e#)))
         (finally
           (while (.isHeldByCurrentThread lock#)
             (.unlock lock#)))))
     nil))

;;; Node Fetching

(defn- read-node-from-file [^RandomAccessFile file address]
  (locking file
    (.seek file address)
    (let [size (.readLong file)
          data (byte-array size)]
      (.read file data)
      (nippy/thaw data nippy-options))))

(defn- load-node [{:keys [db manifest]} id]
  (let [address (manifest id)]
    (if (nil? address)
      {:type :leaf
       :id 1
       :records (sorted-map)}
      (let [data @(:data db)
            cache (:cache data)]
          (if (cache/has? cache address)
            (do
              (swap! (:data db) assoc :cache (cache/hit cache address))
              (cache/lookup cache address))
            (let [loaded-node (read-node-from-file (:file-reader data) address)]
              (swap! (:data db) assoc :cache (cache/miss cache address loaded-node))
              loaded-node))))))

(defn get-node [txn id]
  (or
   ((:dirty-nodes txn) id)
   (load-node txn id)))

;;;;; B+Tree

(defn leaf-node? [node]
  (= :leaf (:type node)))

(defn- get-matching-child [records k]
  (first (rsubseq records <= k)))

;;; Get

(defn get-matching-leaf [txn {:keys [records] :as node} k]
  (if (leaf-node? node)
    node
    (recur txn
           (get-node txn (second (get-matching-child records k)))
           k)))

(defn b+get [txn k]
  (let [root-id (:root-id txn)
        root (get-node txn (:root-id txn))
        leaf (if (leaf-node? root)
               root
               (get-matching-leaf txn root k))]
    ((:records leaf) k)))

;;; Seek

(defn b+seek [txn start end & {:keys [limit]}]
  (let [root (get-node txn (:root-id txn))
        start-node (get-matching-leaf txn root start)
        end-node (get-matching-leaf txn root end)
        results (if (= (:id start-node) (:id end-node))
                  (vec (subseq (:records start-node) >= start <= end))
                  (persistent!
                   (loop [pairs (transient (vec (subseq (:records start-node) >= start)))
                          next-id (:next start-node)]
                     (cond
                       (nil? next-id) pairs
                       (and limit (>= (count pairs) limit)) pairs
                       (= next-id (:id end-node)) (reduce conj! pairs (subseq (:records end-node) <= end))
                       :else (let [next-node (get-node txn next-id)]
                               (recur
                                (reduce conj! pairs (:records next-node))
                                (:next next-node)))))))]
    (if (and limit (> (count results) limit))
      (subvec results 0 limit)
      results)))


;;; Seek Reverse

(defn- track-descent
  [txn node track k]
  (loop [node node
         track track]
    (if (leaf-node? node)
      [node track]
      (let [pred (if (= k ::no-key)
                   (reverse (:records node))
                   (rsubseq (:records node) <= k))]
        (recur (get-node txn (second (first pred)))
               (concat (map second (rest pred)) track))))))


(defn b+seek-reverse [txn start end & {:keys [limit]}]
  (let [root (get-node txn (:root-id txn))
        [node track] (track-descent txn root nil end)
        terminal-node (get-matching-leaf txn root start)
        results
        (persistent!
         (loop [node node
                track track
                pairs (transient (vec (rsubseq (:records node) >= start <= end)))]
           (cond
             (= (:id terminal-node) (:id node)) pairs
             (and limit (>= (count pairs) limit)) pairs
             (empty? track) pairs
             :else (let [[node track] (track-descent txn (get-node txn (first track)) (rest track) ::no-key)
                         pairs (reduce conj! pairs (rsubseq (:records node) >= start))]
                     (recur node track pairs)))))]
    (if (and limit (>= (count results) limit))
      (subvec results 0 limit)
      results)))

;;; Insert

(defn split-records [txn {:keys [id type records] :as node}]
  (let [split-pos (Math/ceil (/ (count records) 2))
        left-records (take split-pos records)
        right-records (drop split-pos records)
        split-key (first (first right-records))
        right-records (if (= type :internal)
                        (assoc-in (vec right-records) [0 0] nil)
                        right-records)
        left-node (assoc node :records (into (sorted-map) left-records))
        left-id id
        txn (update txn :id-counter inc)
        right-id (:id-counter txn)
        right-node {:id right-id :type type :records (into (sorted-map) right-records)}
        right-node (if (leaf-node? left-node)
                     (assoc right-node :next (:next left-node))
                     right-node)
        left-node (if (leaf-node? left-node)
                    (assoc left-node :next right-id)
                    left-node)]
    {:txn (-> txn
              (assoc-in [:dirty-nodes left-id] left-node)
              (assoc-in [:dirty-nodes right-id] right-node))
     :split-key split-key
     :left-id left-id
     :right-id right-id}))

(defn- insert-leaf [txn {:keys [id records] :as node} k v]
  (let [new-records (assoc records k v)
        updated-node (assoc node :records new-records)]
    (if (> order (count new-records))
      (assoc-in txn [:dirty-nodes id] updated-node)
      (split-records txn updated-node))))

(defn handle-split-node [{:keys [txn split-key left-id right-id]} {:keys [id records] :as node}]
  (let [new-records (assoc records split-key right-id)
        updated-node (assoc node :records new-records)]
    (if (>= order (count new-records))
      (assoc-in txn [:dirty-nodes id] updated-node)
      (split-records txn updated-node))))

(defn- insert-internal [txn {:keys [id records] :as node} k v]
  (let [[child-key child-id] (get-matching-child records k)
        child (get-node txn child-id)
        result (if (leaf-node? child)
                 (insert-leaf txn child k v)
                 (insert-internal txn child k v))]
    (if (:split-key result)
      (handle-split-node result node)
      result)))

(defn- handle-split-root [{:keys [txn split-key left-id right-id]}]
  (let [txn (update txn :id-counter inc)
        root-id (:id-counter txn)
        txn (assoc txn :root-id root-id)
        new-node {:id root-id
                  :type :internal
                  :records (sorted-map nil left-id split-key right-id)}]
    (assoc-in txn [:dirty-nodes root-id] new-node)))

(defn b+insert [txn k v]
  (let [root (get-node txn (:root-id txn))
        result (if (leaf-node? root)
                 (insert-leaf txn root k v)
                 (insert-internal txn root k v))]
    (if (:split-key result)
      (handle-split-root result)
      result)))

;;; Remove

(defn- remove-leaf [txn {:keys [id records] :as node} k]
  (let [updated-records (dissoc records k)
        updated-node (assoc node :records updated-records)]
    (if (< (count updated-records) (int (/ order 2)))
      {:combine updated-node
       :txn txn}
      (assoc-in txn [:dirty-nodes id] updated-node))))

(defn- combine-records [mid-key left-node right-node]
  (if (leaf-node? left-node)
    (merge (:records left-node) (:records right-node))
    (let [right-records (:records right-node)
          right-records (assoc right-records mid-key (right-records nil))
          right-records (dissoc right-records nil)]
      (merge (:records left-node) right-records))))

(defn- distribute-records [txn mid-key left-node right-node]
  (let [combined-records (combine-records mid-key left-node right-node)
        split-pos (Math/ceil (/ (count combined-records) 2))
        left-records (take split-pos combined-records)
        right-records (drop split-pos combined-records)
        split-key (first (first right-records))
        right-records (if (leaf-node? right-node)
                        right-records
                        (assoc-in (vec right-records) [0 0] nil))]
    {:distributed-by split-key
     :mid-key mid-key
     :txn
     (-> txn
         (assoc-in [:dirty-nodes (:id left-node)] (assoc left-node :records (into (sorted-map) left-records)))
         (assoc-in [:dirty-nodes (:id right-node)] (assoc right-node :records (into (sorted-map) right-records))))}))

(defn- merge-nodes [txn mid-key left-node right-node]
  (let [combined-records (combine-records mid-key left-node right-node)
        updated-node (assoc left-node :records combined-records)
        updated-node (if (leaf-node? left-node)
                       (assoc updated-node :next (:next right-node))
                       updated-node)]
    {:merged true
     :mid-key mid-key
     :txn
     (-> txn
         (assoc-in [:dirty-nodes (:id updated-node)] updated-node)
         (assoc-in [:dirty-nodes (:id right-node)] nil))}))

(defn- get-siblings-helper [txn parent-records child-key]
  (let [rv (vec parent-records)
        focal-index (first (keep-indexed #(when (= (first %2) child-key) %1) rv))
        [left-key left-id] (if (< 0 focal-index) (nth rv (dec focal-index)))
        [right-key right-id] (if (< focal-index (dec (count rv))) (nth rv (inc focal-index)))]
    {:left-key left-key
     :left-sibling (when left-id (get-node txn left-id))
     :right-key right-key
     :right-sibling (when right-id (get-node txn right-id))}))

(defn- combine-children [txn {:keys [records]} child-key focal-child]
  (let [{:keys [left-key left-sibling right-key right-sibling]} (get-siblings-helper txn records child-key)
        left-count (count (:records left-sibling))
        right-count (count (:records right-sibling))
        min-count (int (/ order 2))]
    (cond
      (> right-count min-count) (distribute-records txn right-key focal-child right-sibling)
      (> left-count min-count) (distribute-records txn child-key left-sibling focal-child)
      (= right-count min-count) (merge-nodes txn right-key focal-child right-sibling)
      (= left-count min-count) (merge-nodes txn child-key left-sibling focal-child))))

(defn- remove-internal [txn {:keys [id records] :as node} k]
  (let [[child-key child-id] (get-matching-child records k)
        child (get-node txn child-id)
        result (if (leaf-node? child)
                 (remove-leaf txn child k)
                 (remove-internal txn child k))]
    (if-let [focal-child (:combine result)]
      (let [{:keys [mid-key distributed-by txn]} (combine-children (:txn result) node child-key focal-child)
            mid-value (get records mid-key)
            updated-records (dissoc records mid-key)
            updated-records (if distributed-by
                              (assoc updated-records distributed-by mid-value)
                              updated-records)
            updated-node (assoc node :records updated-records)]
        (if (< (count updated-records) (int (/ order 2)))
          {:combine updated-node
           :txn txn}
          (assoc-in txn [:dirty-nodes id] updated-node)))
      result)))

(defn b+remove [txn k]
  (let [root-id (:root-id txn)
        root (get-node txn (:root-id txn))]
    (if (leaf-node? root)
      (let [result (remove-leaf txn root k)]
        (if (:combine result)
          (assoc-in (:txn result) [:dirty-nodes root-id] (:combine result))
          result))
      (let [result (remove-internal txn root k)]
        (if (:combine result)
          (let [txn (:txn result)
                node (:combine result)]
            (if (= 1 (count (:records node)))
              (let [new-root-id (second (first (:records node)))]
                (-> txn
                    (assoc :root-id new-root-id)
                    (assoc-in [:dirty-nodes root-id] nil)))
              (assoc-in txn [:dirty-nodes (:id node)] node)))
          result)))))

;;; Util

(defn b+count-all [txn]
  (loop [node-id 1
         total 0]
    (if node-id
      (let [node (get-node txn node-id)]
        (recur (:next node)
               (+ total (count (:records node)))))
      total)))

(defn b+depth [txn]
  (loop [depth 0
         node (get-node txn (:root-id txn))]
    (if (leaf-node? node)
      (inc depth)
      (recur (inc depth) (get-node txn (second (first (:records node))))))))
