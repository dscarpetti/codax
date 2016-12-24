(ns codax.util
  (:require
   [clojure.string :as str]
   [codax.store :as store]
   [vijual :refer [draw-tree]]))

;;;; Print Records

(defn print-records [data]
  (let [processed-result (reduce (fn [acc [k v]]
                                   (let [key (str k)]
                                     (-> acc
                                         (assoc :max-key-length (max (.length key) (:max-key-length acc)))
                                         (assoc :data (conj (:data acc) [key v])))))
                                 {:max-key-length 0
                                  :data []}
                                 data)
        data (:data processed-result)
        max-key-length (:max-key-length processed-result)
        pad (clojure.string/join  (repeat max-key-length "."))]
    (str
     (count (mapv (fn [[k v]]
                      (println (str k "  ." (subs pad (.length k)) ".  "  v)))
                    data))
     " records")))


;;;; Tree Drawing

(defn draw-b-tree-helper [txn {:keys [records] :as node}]
  (if (store/leaf-node? node)
    (vector (str/join " " (keys records)))
    (apply vector
           (cond
              (zero? (count (keys records))) "0!"
              (= 1 (count (keys records))) "_"
              :else (str/trim (str/join " :" (keys records))))
           (map (partial draw-b-tree-helper txn) (map (partial store/get-node txn) (vals records))))))

(defn draw-tx [txn]
  (let [root (store/get-node txn (:root-id txn))]
    (draw-tree [(draw-b-tree-helper txn root)])))

(defn draw [db]
  (store/with-read-transaction [db tx]
    (draw-tx tx)))


;;;; Tree Height/Element Calculations

(defn max-elements [b h]
  (- (Math/pow b h) (Math/pow b (- h 1))))

(defn min-height [b cnt]
  (/
   (Math/log (/ (* cnt b) (- b 1)))
   (Math/log b)))

(defn min-elements [b h]
  (* 2 (- (Math/pow (/ b 2) (- h 1))
          (Math/pow (/ b 2) (- h 2)))))

(defn max-height [b cnt]
  (/ (Math/log (/ (* b b cnt) (- (* 4 b) 8)))
     (Math/log (/ b 2))))
