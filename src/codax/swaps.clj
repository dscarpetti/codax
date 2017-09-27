(ns codax.swaps)

(defn inc-count
  "Used for incrementing a counter that may not be initialized.
  If `x` is not a positive integer, `inc-count` evaluates to 1"
  [x]
  (if (and (integer? x) (pos? x))
    (inc x)
    1))

(defn dec-count
  "Used for decrementing a counter that may not be initialized.
  If `x` is not a positive integer, `dec-count` evaluates to 0"
  [x]
  (if (and (integer? x) (> x 1))
    (dec x)
    0))
