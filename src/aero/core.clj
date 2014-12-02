(ns aero.core
  (:import com.aerospike.client.AerospikeClient
           com.aerospike.client.Host
           com.aerospike.client.policy.WritePolicy
           com.aerospike.client.policy.RecordExistsAction
           com.aerospike.client.Key
           com.aerospike.client.Bin
           java.util.Date
           ))

(def server-ip "")

;;
;; ex1
;;

RecordExistsAction

(defn get-client
  []
  (let
    [client (new AerospikeClient (server-ip) 3000)]
      {
       :client client
       }))

(defn get-write-policy
  [client]
  (let
    [policy (new WritePolicy)]
    (set! (.recordExistsAction policy) (RecordExistsAction/UPDATE))
    (assoc client :write-policy policy)))

(defn kill-client
  [client]
  (.close (client :client)))



(->
 (get-client)
 (get-write-policy)
 (kill-client))





;;
;; ex 2.1
;; create records
;;

(defn from-comma-string [strr]
  (clojure.string/split strr #","))

;; test the string join
(from-comma-string "hi,there")

(defn create-user
  [client data]
  (let [key1 (new Key "test" "users" (data :username))
        bin1 (new Bin "username" (data :username))
        bin2 (new Bin "password" (data :password))
        bin3 (new Bin "gender" (data :gender))
        bin4 (new Bin "region" (data :region))
        bin5 (new Bin "lasttweeted" (data :last-tweeted))
        bin6 (new Bin "tweetcount" (data :tweetcount))
        bin7 (new Bin "interests" (from-comma-string (data :interests)))
        ]
    (.put (client :client) (client :write-policy) key1 (into-array Bin [bin1 bin2 bin3 bin4 bin5 bin6 bin7]))
    client
  ))

(defn create-tweet
  [client data]
  (let [key1 (new Key "test" "tweets" (str (data :username) ":" (data :next-tweetcount)))
        bin1 (new Bin "tweet" (data :tweet))
        bin2 (new Bin "ts" (data :ts))
        bin3 (new Bin "username" (data :username))

        ]
    (.put (client :client) (client :write-policy) key1 (into-array Bin [bin1 bin2 bin3]))
  ))

(def user1 {
            :username "micah"
            :password "passy"
            :gender "m"
            :region "region"
            :last-tweeted (new Date)
            :tweetcount 0
            :interests "code,puppies,cupcakes"

            })

(create-user (-> (get-client) (get-write-policy)) user1)

(def tweet1 {
             :username "micah"
             :next-tweetcount 1
             :ts (new Date)
             :tweet "hi!!!"
             })

(create-tweet (-> (get-client) (get-write-policy)) tweet1)





;;
;; ex 2.2
;; read records
;;

(defn read-user [client username]
  (let
    [key1 (new Key "test" "users" username)
     rec (.get (client :client) nil key1)]
       (if-not (nil? rec)
        (println (format "user: %s password: %s"
                (.getValue rec "username")
                (.getValue rec "password")))
          )))

;; this is found
(read-user (get-client) "micah")

;; this is not found
(read-user (get-client) "jenna")




;;
;; ex 3.2
;;


(defn batch-read
  [client username]
    (let [key1 (new Key "test" "users" username)
          rec (.get (client :client) nil key1)]
          (if-not (nil? rec)
            (println (format "tweetCount: %s" (.getValue rec "tweetcount")))

            ;; not finished

            )))


;; run it
(batch-read (get-client) "micah")


