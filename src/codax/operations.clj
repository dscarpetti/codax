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
  ([tx start-path end-path & {:keys [limit no-decode only-keys only-vals partial]}]
   (let [order-char (if partial nil (char 0x00))
         start (if (empty? start-path)
                 (str (char 0x00))
                 (str (pathwise/partially-encode start-path) order-char))
         end (if (empty? end-path)
               (str (char 0xff))
               (str (pathwise/partially-encode end-path) order-char (char 0xff)))
         results (store/b+seek tx start end :limit limit)]
     (cond
       (and no-decode only-keys) (map first results)
       no-decode results
       only-keys (map #(pathwise/decode (first %)) results)
       only-vals (map second results)
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
  [tx lead-trim start-path end-path limit partial]
  (let [chunk-size (if (and (number? limit) (pos? limit))
                     (max (int (* 1.5 limit)) 10)
                     nil)
        limit (if (and (number? limit)) limit -1)]
    (persistent!
     (loop [chunk (seek tx start-path end-path :limit chunk-size :partial partial :no-decode true)
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
              (rest (seek tx (pathwise/decode last-key) end-path :limit chunk-size :partial partial :no-decode true))
              complete
              active-key
              active-data
              limit))))))))


(defn seek-path
  ([tx path limit]
   (seek-path-chunk tx (count path) path path limit false))

  ([tx path prefix limit]
   (let [seek-path (conj path prefix)]
     (seek-path-chunk tx (count path) seek-path seek-path limit true)))

  ([tx path start-val end-val limit]
   (if (or (nil? end-val) (pos? (compare start-val end-val)))
     []
     (let [start-path (conj path start-val)
           end-path (if (nil? end-val) nil (conj path end-val))]
       (seek-path-chunk tx (count path) start-path end-path limit false)))))

;;;;;;;

(defn- assemble-seek-simple [lead-trim data]
  (loop [raw data
         complete []
         active-key ::none
         active-data nil]
    (if (empty? raw)
      (if (= active-key ::none)
        complete
        (conj complete [active-key active-data]))
      (let [[raw-k v] (first raw)
            [k & ks] (drop lead-trim (pathwise/decode raw-k))]
        (if (or (= active-key k) (= active-key ::none))
          (recur (rest raw)
                 complete
                 k
                 (if (empty? ks) v (assoc-in active-data ks v)))
          (recur (rest raw)
                 (conj complete [active-key active-data])
                 k
                 (if (empty? ks) v (assoc-in {} ks v))))))))


(defn seek-path-simple
  ([tx path]
   (assemble-seek-simple (count path)
                       (seek tx path path :no-decode true)))
  ([tx path prefix]
   (let [seek-path (conj path prefix)]
     (assemble-seek-simple (count path)
                         (seek tx seek-path seek-path :no-decode true :partial true))))

  ([tx path start-val end-val]
   (let [start-path (conj path start-val)
         end-path (conj path end-val)]
     (assemble-seek-simple (count path)
                         (seek tx start-path end-path :no-decode true)))))


;;;;;

(defn- validate-path [tx path]
  (when (empty? path) (throw (ex-info "Invalid Path" {:cause :empty-path
                                                      :message "You cannot modify the empty (root) path."})))
  (loop [remaining-path (pop path)
         validated-paths (or (:validated-paths tx) #{})]
    (if (or (zero? (count remaining-path))
            (contains? validated-paths remaining-path))
      (assoc tx :validated-paths validated-paths)
      (if-let [val (get-val tx remaining-path)]
        (throw (ex-info "Occupied Path" {:cause :non-map-element
                                         :message "Could not extend the path because a non-map element was encountered."
                                         :attempted-path path
                                         :element-at remaining-path
                                         :element-value val}))
        (recur (pop remaining-path) (conj validated-paths remaining-path))))))

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
