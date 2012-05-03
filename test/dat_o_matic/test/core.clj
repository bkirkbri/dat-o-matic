(ns dat-o-matic.test.core
  (:use [dat-o-matic.core])
  (:use [datomic.api :only [q db] :as d])
  (:use [clojure.test]))

(deftest temp-conn-test
  (is (db (temp-conn)) "Can create an unnamed temporary database.")
  (is (db (temp-conn "testing")) "Can create a named temporary database."))

(deftest transact-test
  (let [conn (temp-conn)
        txn [{:db/id #db/id[:db.part/user] :db/ident :test-entity}]
        bad-txn (assoc-in txn [0 :not-an-attr] 123)]
    (is (= true (transact! conn txn)) "Can issue transactions.")
    (is (thrown? Exception (transact! conn bad-txn)) "Exception is thrown on bad transact.")
    (is (= true (safe-transact! conn txn)) "Can issue a safe transaction.")
    (is (= false (safe-transact! conn bad-txn)) "Can issue a failing safe transaction.")))

(deftest create-part-test
  (let [part-txn {:db/id #db/id[:db.part/db]
                  :db/ident :test-part
                  :db.install/_partition :db.part/db}
        conn (temp-conn)
        txn [{:db/id #db/id[:test-part] :db/ident :test-entity}]]
    (is (= (dissoc (first (create-partition-txn :test-part)) :db/id)
           (dissoc part-txn :db/id)) "Transaction data for partition is correct.")
    (is (thrown? Exception (transact! conn txn)) "Can't add to non-existent partition.")
    (is (= true (create-partition! conn :test-part)))
    (is (= true (transact! conn txn)) "Can add after partition exists.")))

(deftest create-attr-test
  (let [attr-txn {:db/id #db/id[:db.part/db]
                  :db/ident :myattr
                  :db/unique :db.unique/identity
                  :db/index true
                  :db/fulltext true
                  :db/doc "Something"
                  :db/valueType :db.type/string
                  :db/cardinality :db.cardinality/one
                  :db.install/_attribute :db.part/db}
        conn (temp-conn)
        txn [{:db/id #db/id [:db.part/user] :db/ident :test-entity :myattr "yo ho ho and a bottle of"}]
        attr-args [:myattr :type :string :doc "Something" :unique :identity :index true :fulltext true]]
    (is (= (dissoc (first (apply create-attribute-txn attr-args)) :db/id)
           (dissoc attr-txn :db/id)) "Transaction data for attribute is correct.")
    (is (thrown? Exception (transact! conn txn)) "Can't add a non-existent attribute.")
    (is (= true (apply create-attribute! conn attr-args)))
    (is (= true (transact! conn txn)) "Can add after attribute exists.")))

(deftest create-entity-test
  (let [conn (temp-conn)
        attrs {:name "Lucifugus" :desc "Eternally frozen..." :uid 42}]
    (create-partition! conn :mine)
    (create-attributes! conn
                        [:name :type :string]
                        [:desc :type :string]
                        [:uid :type :long :index true :unique :identity])
    (create-entity! conn :mine attrs)
    (let [db (db conn)
          eid (ffirst (q '[:find ?e :where [?e :uid 42]] db))
          e (d/entity db eid)]
      (doseq [k (keys attrs)]
        (is (= (get attrs k) (get e k)))))))

(deftest ensure-db-test
  (let [conn (temp-conn)]
    (is (isa? (class (ensure-db conn)) datomic.db.Db) "ensure-db works on connections.")
    (is (isa? (class (ensure-db (db conn))) datomic.db.Db) "ensure-db works on databases.")))

;; TODO more complete testing of list-attributes
(deftest list-attr-test
  (let [conn (temp-conn)]
    (is (list-attributes conn) "Can list all attributes.")
    (is (list-attributes conn #"^:db") "Can list attributes matching regex.")
    (is (empty? (list-attributes conn #"unlikely")) "Doesn't return non-matching attributes.")))