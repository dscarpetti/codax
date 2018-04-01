(ns codax.operations
  (:require
   [codax.store :as store]
   [codax.pathwise :as pathwise]))

(defn get-val [tx path]
  (store/b+get tx (pathwise/encode path)))

(defn delete-val [tx path]
  (store/b+remove tx (pathwise/encode path)))

(defn put-val [tx path v]
  (store/b+insert tx (pathwise/encode path) v))

(defn update-val [tx path f & args]
  (let [k (pathwise/encode path)
        old-val (store/b+get tx k)
        new-val (apply f old-val args)]
    (store/b+insert tx k new-val)))

(defn seek
  ([tx] (seek tx nil nil))
  ([tx path] (seek tx path path))
  ([tx start-path end-path & {:keys [no-decode only-keys]}]
   (let [start (if (empty? start-path)
                 (str (char 0x00))
                 (str (pathwise/partially-encode start-path) (char 0x00)))
         end (if (empty? end-path)
               (str (char 0xff))
               (str (pathwise/partially-encode end-path) (char 0x00) (char 0xff)))
         results (store/b+seek tx start end)]
     (cond
       (and no-decode only-keys) (map first results)
       no-decode results
       only-keys (map #(pathwise/decode (first %)) results)
       :else (map #(update % 0 pathwise/decode) results)))))


(defn- reduce-assoc [coll [k v]]
  (if (not (vector? k))
    coll
    (assoc-in coll k v)))

(defn collect
  "Returns a map assembled from the keys beginning with the
  that begin with the complete supplied `path` conjoined with
  the transaction prefix.

  Ex. If the transaction `tx` has no prefix & the database contains:

  [ key ]                   | val
  -------------------------------
  [:dept :art      :alice]  |  1
  [:dept :bio-tech   :bob]  |  2
  [:dept :biology  :chloe]  |  3
  [:dept :biology :daniel]  |  4
  [:dept :biology  :ellen]  |  5
  [:dept :botany   :frank]  |  6
  -------------------------------

  (collect tx [] -> {:dept
                     {:art {:alice 1}
                      :bio-tech {:bob 2}
                      :biology {:chloe 3
                                :daniel 4
                                :ellen 5}
                      :botant  {:frank 6}}}


  (collect tx [:dept :biology]) -> {:chloe 3
                                    :daniel 4
                                    :ellen 5}}


  Ex. If, however the transaction prefix was `[:dept]`:

  (seek tx [:biology]) -> {:chloe 3
                           :daniel 4
                           :ellen 5}}"
  [tx path]
  (get-in (reduce reduce-assoc {} (seek tx path)) path))



;;;;;;;;;;;;


(defn- seek-chunk [tx start end chunk-size reverse]
  (if reverse
    (store/b+seek-reverse tx start end :limit chunk-size)
    (store/b+seek tx start end :limit chunk-size)))


(defn- assemble-chunk [lead-trim data complete active-key active-data limit last-key]
  (if (or (= limit 0) (empty? data))
    [complete active-key active-data limit last-key]
    (let [[raw-k v] (first data)
          [k & ks] (drop lead-trim (pathwise/decode raw-k))]
      (if (or (= active-key k) (= active-key ::none))
        (recur lead-trim
               (rest data)
               complete
               k
               (if (empty? ks) v (assoc-in active-data ks v))
               limit
               raw-k)
        (recur lead-trim
               (rest data)
               (conj! complete [active-key active-data])
               k
               (if (empty? ks) v (assoc-in {} ks v))
               (dec limit)
               raw-k)))))

(defn- seek-path-chunk
  [tx lead-trim start end limit reverse]
  (let [chunk-size (if (and (number? limit) (pos? limit))
                     (max (int limit) 10)
                     nil)
        limit (if (and (number? limit)) limit -1)]
    (persistent!
     (loop [chunk (seek-chunk tx start end chunk-size reverse)
            results (transient [])
            active-key ::none
            active-data nil
            limit limit]
       (if (or (= limit 0) (empty? chunk))
         (if (= active-key ::none)
           results
           (conj! results [active-key active-data]))
         (let [prev-limit limit
               [complete active-key active-data limit last-key]
               (assemble-chunk lead-trim chunk results active-key active-data limit nil)]
           (if (= limit 0)
             complete
             (recur
              (if reverse
                (rest (seek-chunk tx start last-key chunk-size true))
                (rest (seek-chunk tx last-key end chunk-size false)))
              complete
              active-key
              active-data
              limit))))))))

(defn- encode-seek-path [path & cs]
  (apply str (pathwise/partially-encode path) (map char cs)))

(defmacro ^:private assemble-seek [start end]
  `(seek-path-chunk ~'tx (count ~'path)
                    (str (codax.pathwise/partially-encode ~start) (char 0x00))
                    (str (codax.pathwise/partially-encode ~end) (char 0x00) (char 0xff))
                    ~'limit ~'reverse))

(defn seek-path [tx path limit reverse]
  (assemble-seek path path))

(defn seek-from [tx path start-val limit reverse]
  (assemble-seek (conj path start-val) path))

(defn seek-to [tx path end-val limit reverse]
  (assemble-seek path (conj path end-val)))

(defn seek-range [tx path start-val end-val limit reverse]
  (if (pos? (compare start-val end-val))
    []
    (assemble-seek (conj path start-val) (conj path end-val))))

(defn seek-prefix [tx path val-prefix limit reverse]
  (let [seek-path (conj path val-prefix)
        start (encode-seek-path seek-path)
        end (encode-seek-path seek-path 0xff)]
    (seek-path-chunk tx (count path) start end limit reverse)))

(defn seek-prefix-range [tx path start-prefix end-prefix limit reverse]
  (if (pos? (compare start-prefix end-prefix))
    []
    (let [start (encode-seek-path (conj path start-prefix))
          end (encode-seek-path (conj path end-prefix) 0xff)]
      (seek-path-chunk tx (count path) start end limit reverse))))

;;;;;

(defn- validate-path [tx path]
  (when (empty? path) (throw (ex-info "Invalid Path" {:cause :empty-path
                                                      :message "You cannot modify the empty (root) path."})))
  (loop [remaining-path (pop path)
         validated-paths (or (:validated-paths tx) #{})]
    (if (or (zero? (count remaining-path))
            (contains? validated-paths remaining-path))
      (assoc tx :validated-paths validated-paths)
      (let [val (get-val tx remaining-path)]
        (if (not (nil? val))
          (throw (ex-info "Occupied Path" {:cause :non-map-element
                                           :message "Could not extend the path because a non-map element was encountered."
                                           :attempted-path path
                                           :element-at remaining-path
                                           :element-value val}))
          (recur (pop remaining-path) (conj validated-paths remaining-path)))))))

(defn- clear-path [tx path]
  (reduce (fn [tx raw-key] (store/b+remove tx raw-key))
          tx
          (seek tx path path :only-keys true :no-decode true)))

(defn- assoc-helper [tx path x]
  (if (map? x)
    (reduce-kv (fn [tx k v] (assoc-helper tx (conj path k) v)) tx x)
    (put-val tx path x)))

(defn assoc-path [tx path v]
  (-> tx
      (validate-path path)
      (clear-path path)
      (assoc-helper path v)))

(defn- collect-delete [tx path]
  (let [values (seek tx path path :no-decode true)]
    {:original (get-in (reduce reduce-assoc {} (map #(update % 0 pathwise/decode) values)) path)
     :tx (reduce (fn [t [raw-key _]] (store/b+remove tx raw-key)) tx values)}))

(defn update-path [tx path f & args]
  (let [tx (validate-path tx path)
        {:keys [tx original]} (collect-delete tx path)
        updated (apply f original args)]
    (assoc-helper tx path updated)))

(defn delete-path [tx path]
  (when (empty? path) (throw (ex-info "Invalid Path" {:cause :empty-path
                                                      :message "You cannot clear the empty (root) path."})))
  (clear-path tx path))


;;;;;

(defn next-id!
  "Function generates auto incremented id starting from 0 for the specified entity.
  Parameters:
    db -- opened database
    entity -- the entity for which the id should be generated.
  Return:
    The value of the next available id for the specified entity.
  Usage examples:
    (next-id! db :person) => 0
    (-> db (next-id! :person)) => 1
  "
  [ db entity ]
  (let [n (atom nil)]    
    (c/with-write-transaction [db tx]
      
      ;; Read previous index value. If it is absent -- use 0 instead;
      (reset! n (or (c/get-at! db [:codax/indicies entity]) 0))
      
      ;; Create or update the next index value with incremented previous value
      (c/assoc-at tx [:codax/indicies entity] (inc @n)))

    ;; Return original index value
    @n))

