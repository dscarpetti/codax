(ns codax.swaps)

(defn inc-count [x]
  (if (and (integer? x) (pos? x))
    (inc x)
    1))

(defn dec-count [x]
  (if (and (integer? x) (> x 1))
    (dec x)
    nil))
