(ns codax.prefix)

(defn set-prefix [tx prefix]
  (assoc tx :prefix
         (cond
           (nil? prefix) []
           (vector? prefix) prefix
           (coll? prefix) (vec prefix)
           :else [prefix])))

(defn prefix-path
  "Helper function for prefixing a path with the `:prefix` of the transaction `txn`.
  Generally for internal library use only"
  [tx k]
  (cond
    (nil? k) (:prefix tx)
    (coll? k) (vec (concat (:prefix tx) (vec k)))
    :else (conj (:prefix tx) k)))

(defn clear-prefix
  "Clears the prefix of the transaction. This is the
  default state of a new transaction object."
  [tx]
  (set-prefix tx nil))

(defn extend-prefix
  "Conjoins the current transaction prefix with `prefix-extentension`."
  [tx prefix-extension]
  (if (nil? prefix-extension)
    tx
    (assoc tx :prefix
           (vec (concat (:prefix tx) (if (coll? prefix-extension) prefix-extension [prefix-extension]))))))

(defn push-prefix
  "Stores the current transaction prefix in the prefix stack, then
  sets the supplied `prefix` as the active transaction prefix."
  [tx prefix]
  (-> tx
      (assoc :prefix-stack (cons (:prefix tx) (:prefix-stack tx)))
      (set-prefix prefix)))

(defn pop-prefix
  "Replaces the active transaction prefix with the top item on the
  prefix stack. Throws an error if the stack is empty."
  [tx]
  (assert (not (empty? (:prefix-stack tx))) "Prefix stack is empty. More pushes than pops.")
  (-> tx
      (set-prefix (first (:prefix-stack tx)))
      (assoc :prefix-stack (rest (:prefix-stack tx)))))

(defn push-extend-prefix
  "Stores the current transaction prefix in the prefix stack, then
  sets the transaction prefix to that prefix conjoined with supplied
  `prefix-extension`"
  [tx prefix-extension]
  (-> tx
      (assoc :prefix-stack (cons (:prefix tx) (:prefix-stack tx)))
      (set-prefix (concat (:prefix tx) (if (coll? prefix-extension) prefix-extension [prefix-extension])))))
