(ns dat-o-matic.test.core
  (:use [dat-o-matic.core])
  (:use [datomic.api :only [q db] :as d])
  (:use [clojure.test]))

(deftest temp-db-test
  (is (db (temp-db)) "Can create an unnamed temporary database.")
  (is (db (temp-db "testing")) "Can create a named temporary database."))

(deftest transact-test
  (let [test-conn (temp-db)
        txn [{:db/id #db/id[:db.part/user] :db/ident :test-entity}]
        bad-txn (assoc-in txn [0 :not-an-attr] 123)]
    (is (= true (transact test-conn txn)) "Can issue transactions.")
    (is (thrown? Exception (transact test-conn bad-txn)) "Exception is thrown on bad transact.")
    (is (= true (safe-transact test-conn txn)) "Can issue a safe transaction.")
    (is (= false (safe-transact test-conn bad-txn)) "Can issue a failing safe transaction.")))

(deftest new-part-test
  (let [part-txn {:db/id #db/id[:db.part/db]
                  :db/ident :test-part
                  :db.install/_partition :db.part/db}
        test-conn (temp-db)
        txn [{:db/id #db/id[:test-part] :db/ident :test-entity}]]
    (is (= (dissoc (first (new-partition-txn :test-part)) :db/id)
           (dissoc part-txn :db/id)) "Transaction data for partition is correct.")
    (is (thrown? Exception (transact test-conn txn)) "Can't add to non-existent partition.")
    (is (= true (new-partition test-conn :test-part)))
    (is (= true (transact test-conn txn)) "Can add after parition exists.")))

(deftest new-attr-test
  (let [attr-txn {:db/id #db/id[:db.part/db]
                  :db/ident :myattr
                  :db/unique :db.unique/identity
                  :db/index true
                  :db/fulltext true
                  :db/doc "Something"
                  :db/valueType :db.type/string
                  :db/cardinality :db.cardinality/one
                  :db.install/_attribute :db.part/db}
        test-conn (temp-db)
        txn [{:db/id #db/id [:db.part/user] :db/ident :test-entity :myattr "yo ho ho and a bottle of"}]
        attr-args [:myattr :type :string :doc "Something" :unique :identity :index true :fulltext true]]
    (is (= (dissoc (first (apply new-attribute-txn attr-args)) :db/id)
           (dissoc attr-txn :db/id)) "Transaction data for attribute is correct.")
    (is (thrown? Exception (transact test-conn txn)) "Can't add a non-existent attribute.")
    (is (= true (apply new-attribute test-conn attr-args)))
    (is (= true (transact test-conn txn)) "Can add after attribute exists.")))

(deftest ensure-db-test
  (let [test-conn (temp-db)]
    (is (isa? (class (ensure-db test-conn)) datomic.db.Db) "ensure-db works on connections.")
    (is (isa? (class (ensure-db (db test-conn))) datomic.db.Db) "ensure-db works on databases.")))