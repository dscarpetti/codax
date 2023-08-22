(ns codax.example-test
  (:require  [clojure.test :refer :all]
             [codax.core :as c]))


(defn simple-use-body [db]

  (is (=
       (c/assoc-at! db [:assets :people] {0 {:name "Alice"
                                             :occupation "Programmer"
                                             :age 42}
                                          1 {:name "Bob"
                                             :occupation "Writer"
                                             :age 27}})
       {0 {:name "Alice"
           :occupation "Programmer"
           :age 42}
        1 {:name "Bob"
           :occupation "Writer"
           :age 27}}))

  (is (=
       (c/get-at! db [:assets :people 0])
       {:name "Alice" :occupation "Programmer" :age 42}))


  (is (=
       (c/update-at! db [:assets :people 1 :age] inc)
       28))


  (is (=
       (c/merge-at! db [:assets] {:tools {"hammer" true
                                          "keyboard" true}})
       {:people {0 {:name "Alice"
                    :occupation "Programmer"
                    :age 42}
                 1 {:name "Bob"
                    :occupation "Writer"
                    :age 28}}
        :tools {"hammer" true, "keyboard" true}}))


  (is (=
       (c/get-at! db [:assets])
       {:people {0 {:name "Alice"
                    :occupation "Programmer"
                    :age 42}
                 1 {:name "Bob"
                    :occupation "Writer"
                    :age 28}}
        :tools {"hammer" true
                "keyboard" true}})))


(defn transaction-example-body [db]
  (let [add-user (fn [username ts]
                   (c/with-write-transaction [db tx]
                     (when (c/get-at tx [:usernames username] )
                       (throw (Exception. "username already exists")))
                     (let [user-id (c/get-at tx [:counters :id])
                           user {:id user-id
                                 :username username
                                 :timestamp ts}]
                       (-> tx
                           (c/assoc-at [:users user-id] user)
                           (c/assoc-at [:usernames username] user-id)
                           (c/update-at [:counters :id] inc)
                           (c/update-at [:counters :users] inc)))))
        get-user (fn [username]
                   (c/with-read-transaction [db tx]
                     (when-let [user-id (c/get-at tx [:usernames username])]
                       (c/get-at tx [:users user-id]))))
        rename-user (fn [username new-username]
                      (c/with-write-transaction [db tx]
                        (when (c/get-at tx [:usernames new-username] )
                          (throw (Exception. "username already exists")))
                        (when-let [user-id (c/get-at tx [:usernames username])]
                          (-> tx
                              (c/dissoc-at [:usernames username])
                              (c/assoc-at [:usernames new-username] user-id)
                              (c/assoc-at [:users user-id :username] new-username)))))
        remove-user (fn [username]
                      (c/with-write-transaction [db tx]
                        (when-let [user-id (c/get-at tx [:usernames username])]
                          (-> tx
                              (c/dissoc-at [:username username])
                              (c/dissoc-at [:users user-id])
                              (c/update-at [:counters :users] dec)))))]

    ;; init
    (is (=
         (c/with-write-transaction [db tx]
           (c/assoc-at tx [:counters] {:id 0 :users 0}))
         nil))

    ;; edit users

    (is (=
         (c/get-at! db)
         {:counters {:id 0, :users 0}}))


    (is (=
         (add-user "charlie" 1484529469567)
         nil))

    (is (=
         (c/get-at! db)
         {:counters {:id 1, :users 1},
          :usernames {"charlie" 0},
          :users {0 {:id 0, :timestamp 1484529469567, :username "charlie"}}}))


    (is (=
         (add-user "diane" 1484529603444)
         nil))

    (is (=
         (c/get-at! db)
         {:counters {:id 2, :users 2},
          :usernames {"charlie" 0, "diane" 1},
          :users
          {0 {:id 0, :timestamp 1484529469567, :username "charlie"},
           1 {:id 1, :timestamp 1484529603444, :username "diane"}}}))


    (is (=
         (rename-user "charlie" "chuck")
         nil))

    (is (=
         (c/get-at! db)
         {:counters {:id 2, :users 2},
          :usernames {"chuck" 0, "diane" 1},
          :users
          {0 {:id 0, :timestamp 1484529469567, :username "chuck"},
           1 {:id 1, :timestamp 1484529603444, :username "diane"}}}))


    (is (=
         (remove-user "diane"))
        nil)

    (is (=
         (c/get-at! db)
         {:counters {:id 2, :users 1},
          :usernames {"chuck" 0, "diane" 1},
          :users {0 {:id 0, :timestamp 1484529469567, :username "chuck"}}}))))

(defn directory-example-body [db]
  (c/assoc-at! db [:directory]
               {"Alice" {:ext 247, :dept "qa"}
                "Barbara" {:ext 228, :dept "qa"}
                "Damian" {:ext 476, :dept "hr"}
                "Adam" {:ext 357, :dept "hr"}
                "Frank" {:ext 113, :dept "hr"}
                "Bill" {:ext 234, :dept "sales"}
                "Evelyn" {:ext 337, :dept "dev"}
                "Chuck" {:ext 482, :dept "sales"}
                "Emily" {:ext 435, :dept "dev"}
                "Diane" {:ext 245, :dept "dev"}
                "Chelsea" {:ext 345, :dept "qa"}
                "Bob" {:ext 326, :dept "sales"}})

  ;; - seek-at -

  (is (=
       (c/seek-at! db [:directory])
       [["Adam" {:dept "hr", :ext 357}]
        ["Alice" {:dept "qa", :ext 247}]
        ["Barbara" {:dept "qa", :ext 228}]
        ["Bill" {:dept "sales", :ext 234}]
        ["Bob" {:dept "sales", :ext 326}]
        ["Chelsea" {:dept "qa", :ext 345}]
        ["Chuck" {:dept "sales", :ext 482}]
        ["Damian" {:dept "hr", :ext 476}]
        ["Diane" {:dept "dev", :ext 245}]
        ["Emily" {:dept "dev", :ext 435}]
        ["Evelyn" {:dept "dev", :ext 337}]
        ["Frank" {:dept "hr", :ext 113}]]))

  (is (=
       (c/seek-at! db [:directory] :limit 3)
       [["Adam" {:dept "hr", :ext 357}]
        ["Alice" {:dept "qa", :ext 247}]
        ["Barbara" {:dept "qa", :ext 228}]]))

  (is (=
       (c/seek-at! db [:directory] :limit 3 :reverse true)
       [["Frank" {:ext 113, :dept "hr"}]
        ["Evelyn" {:ext 337, :dept "dev"}]
        ["Emily" {:ext 435, :dept "dev"}]]))


  ;; - seek-prefix -

  (is (=
       (c/seek-prefix! db [:directory] "B")
       [["Barbara" {:dept "qa", :ext 228}]
        ["Bill" {:dept "sales", :ext 234}]
        ["Bob" {:dept "sales", :ext 326}]]))

  (is (=
       (c/seek-prefix-range! db [:directory] "B" "D")
       [["Barbara" {:dept "qa", :ext 228}]
        ["Bill" {:dept "sales", :ext 234}]
        ["Bob" {:dept "sales", :ext 326}]
        ["Chelsea" {:dept "qa", :ext 345}]
        ["Chuck" {:dept "sales", :ext 482}]
        ["Damian" {:dept "hr", :ext 476}]
        ["Diane" {:dept "dev", :ext 245}]])))


(defn messaging-example-body [db]
  (let [post-message! nil
        post-message! (fn
                        ([user body]
                         (post-message! (java.time.Instant/now) user body))
                        ([inst user body]
                         (c/assoc-at! db [:messages inst] {:user user
                                                           :body body})))

        process-messages (fn
                           [messages]
                           (map (fn [[inst m]] (assoc m :time (str inst))) messages))


        get-messages-before (fn [ts]
                              (process-messages
                               (c/seek-to! db [:messages] (.toInstant ts))))

        get-messages-after (fn [ts]
                             (process-messages
                              (c/seek-from! db [:messages] (.toInstant ts))))

        get-messages-between (fn [start-ts end-ts]
                               (process-messages
                                (c/seek-range! db [:messages] (.toInstant start-ts) (.toInstant end-ts))))

        get-recent-messages (fn [n]
                              (-> (c/seek-at! db [:messages] :limit n :reverse true)
                                  process-messages ;;
                                  reverse))



        simulate-message! (fn [date-time user body]
                            (post-message! (.toInstant date-time) user body))]

    (simulate-message! #inst "2020-06-06T11:01" "Bobby" "Hello")
    (simulate-message! #inst "2020-06-06T11:02" "Alice" "Welcome, Bobby")
    (simulate-message! #inst "2020-06-06T11:03" "Bobby" "I was wondering how codax seeking works?")
    (simulate-message! #inst "2020-06-06T11:07" "Alice" "Please be more specific, have you read the docs/examples?")
    (simulate-message! #inst "2020-06-06T11:08" "Bobby" "Oh, I guess I should do that.")

    (simulate-message! #inst "2020-06-07T14:30" "Chuck" "Anybody here?")
    (simulate-message! #inst "2020-06-07T14:35" "Chuck" "Guess not...")

    (simulate-message! #inst "2020-06-08T16:50" "Bobby" "Okay, so I read the docs. What is the :reverse param for?")
    (simulate-message! #inst "2020-06-08T16:55" "Alice" "Basically, it seeks from the end and works backwards")
    (simulate-message! #inst "2020-06-08T16:56" "Bobby" "Why would I do that?")
    (simulate-message! #inst "2020-06-08T16:57" "Alice" "Well, generally it is used to grab just the end of a long dataset.")


    (is (=
         (get-recent-messages 3)
         (list
          {:user "Alice" :time "2020-06-08T16:55:00Z" :body "Basically, it seeks from the end and works backwards"}
          {:user "Bobby" :time "2020-06-08T16:56:00Z" :body "Why would I do that?"}
          {:user "Alice" :time "2020-06-08T16:57:00Z" :body "Well, generally it is used to grab just the end of a long dataset." })))

    (is (=
         (get-messages-after #inst "2020-06-07T14:32")
         (list
          {:user "Chuck" :time "2020-06-07T14:35:00Z" :body "Guess not..."}
          {:user "Bobby" :time "2020-06-08T16:50:00Z" :body "Okay, so I read the docs. What is the :reverse param for?"}
          {:user "Alice" :time "2020-06-08T16:55:00Z" :body "Basically, it seeks from the end and works backwards"}
          {:user "Bobby" :time "2020-06-08T16:56:00Z" :body "Why would I do that?"}
          {:user "Alice" :time "2020-06-08T16:57:00Z" :body "Well, generally it is used to grab just the end of a long dataset." })))

    (is (=
         (get-messages-before #inst "2020-06-06T11:05")
         (list
          {:user "Bobby" :time "2020-06-06T11:01:00Z" :body "Hello"}
          {:user "Alice" :time "2020-06-06T11:02:00Z" :body "Welcome, Bobby"}
          {:user "Bobby" :time "2020-06-06T11:03:00Z" :body "I was wondering how codax seeking works?"})))


    (is (=
         (get-messages-between #inst "2020-06-07"
                               #inst "2020-06-07T23:59")
         (list
          {:user "Chuck" :time "2020-06-07T14:30:00Z" :body "Anybody here?"}
          {:user "Chuck" :time "2020-06-07T14:35:00Z" :body "Guess not..."})))))



(deftest simple-use
  (c/destroy-database! "test-databases/example-database")
  (let [db (c/open-database! "test-databases/example-database")]
    (simple-use-body db)
    (c/close-database! db)
    (c/destroy-database! "test-databases/example-database")))

(deftest transaction-example
  (c/destroy-database! "test-databases/example-database")
  (let [db (c/open-database! "test-databases/example-database")]
    (transaction-example-body db)
    (c/close-database! db)
    (c/destroy-database! "test-databases/example-database")))


(deftest directory-example
  (c/destroy-database! "test-databases/example-database")
  (let [db (c/open-database! "test-databases/example-database")]
    (directory-example-body db)
    (c/close-database! db)
    (c/destroy-database! "test-databases/example-database")))

(deftest messaging-example
  (c/destroy-database! "test-databases/example-database")
  (let [db (c/open-database! "test-databases/example-database")]
    (messaging-example-body db)
    (c/close-database! db)
    (c/destroy-database! "test-databases/example-database")))


(deftest bulk
  (c/destroy-database! "test-databases/example-database")
  (let [db (c/open-database! "test-databases/example-database")]
    (transaction-example-body db)
    (simple-use-body db)
    (directory-example-body db)
    (messaging-example-body db)
    ;;(clojure.pprint/pprint (c/get-at! db))
    (c/close-database! db)
    (c/destroy-database! "test-databases/example-database")))
