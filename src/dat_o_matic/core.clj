(ns dat-o-matic.core
  (:use [datomic.api :only [q db] :as d])
  (:import [java.util UUID]))

(defn temp-db
  "Returns a new in-memory database connection."
  [& args]
  (let [name (or (first args) (str (UUID/randomUUID)))
        uri (str "datomic:mem://" name)]
    (d/create-database uri)
    (d/connect uri)))

(defn transact
  "Execute a transaction synchronously and return true or throw an Exception."
  [conn txn]
  (try 
    @(d/transact conn txn)
    (catch Exception e (throw e))))

(defn safe-transact
  "Execute a transaction synchronously and return true or false, logging any Exception."
  [conn txn]
  (try
    @(d/transact conn txn)
    (catch Exception e (println e) false)))

(defn new-partition-txn
  "Returns transaction data that will create and install the requested partition."
  [part-id]
  [{:db/id #db/id[:db.part/db]
    :db/ident part-id
    :db.install/_partition :db.part/db}])

(defn new-partition
  "Creates and installs the requested partition."
  [conn part-id]
  (safe-transact conn (new-partition-txn part-id)))

(defn new-attribute-txn
  "Returns transaction data that will create and install the requested attribute."
  [attr-id & args]
  (let [opts (apply hash-map args)
        type (:type opts)
        _ (when-not type (throw (RuntimeException. "No :type for new-attribute-txn")))
        type (keyword (str "db.type/" (name type)))
        cardinality (:cardinality opts :one)
        cardinality (keyword (str "db.cardinality/" (name cardinality)))
        index (:index opts false)
        fulltext (:fulltext opts false)
        unique (:unique opts false)
        unique (and unique (keyword (str "db.unique/" (name unique))))
        doc (:doc opts)
        component (:component opts)
        no-history (:no-history opts)]
    [(merge {:db/id #db/id[:db.part/db]
             :db/ident attr-id
             :db/valueType type
             :db/cardinality cardinality
             :db.install/_attribute :db.part/db}
            (when doc {:db/doc doc})
            (when unique {:db/unique unique})
            (when index {:db/index index})
            (when fulltext {:db/fulltext fulltext})
            (when (and component (= :db.type/ref type)) {:db/isComponent true})
            (when no-history {:db/noHistory true}))]))

(defn new-attribute
  "Creates and installs the requested partition, returns false on failure."
  [conn & args]
  (safe-transact conn (apply new-attribute-txn args)))

(defn ensure-db
  "Given a Datomic database or connection, return a database."
  [db-or-conn]
  (if (isa? (class db-or-conn) datomic.db.Db) db-or-conn (db db-or-conn)))

(defn describe-attribute
  "Returns a map of facts about an attribute."
  [db-or-conn attr-id]
  (let [e (q `[:find ?e
               :where [[?e :db/ident ~attr-id]
                       [?e :db.install/_attribute :db.part/db]]] (ensure-db db-or-conn))]
    e))

