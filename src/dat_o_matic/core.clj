(ns dat-o-matic.core
  (:use [datomic.api :only [q db] :as d])
  (:require [clojure.pprint :as pp])
  (:require [clojure.string :as str])
  (:import [java.util UUID]))

(defn temp-conn
  "Returns a new in-memory database connection."
  [& args]
  (let [name (or (first args) (str (UUID/randomUUID)))
        uri (str "datomic:mem://" name)]
    (d/create-database uri)
    (d/connect uri)))

(defn transact!
  "Execute a transaction synchronously and return true or throw an Exception."
  [conn txn]
  (try 
    @(d/transact conn txn)
    (catch Exception e (throw e))))

;; TODO: how to handle Exceptions?
;; (println) is not the right way, but it lets us see them in a REPL for now...
(defn safe-transact!
  "Execute a transaction synchronously and return true or false, logging any Exception."
  [conn txn]
  (try
    @(d/transact conn txn)
    (catch Exception e (println e) false)))

(defn create-partition-txn
  "Returns transaction data that will create and install the requested partition."
  [part-id]
  [{:db/id #db/id[:db.part/db]
    :db/ident part-id
    :db.install/_partition :db.part/db}])

(defn create-partition!
  "Creates and installs the requested partition."
  [conn part-id]
  (transact! conn (create-partition-txn part-id)))

(defn create-attribute-txn
  "Returns transaction data that will create and install the requested attribute."
  [attr-id & {:as opts}]
  (let [type (:type opts)
        _ (when-not type (throw (RuntimeException. "No :type for create-attribute-txn")))
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

(defn create-attribute!
  "Creates and installs the requested attribute, returns false on failure.

   Valid options are:
     :type :string, :boolean, :long, :bigint, :float, :double,
           :bigdec, :ref, :instant, :uuid, :uri
     :cardinality :one or :many
     :doc docstring
     :unique :value or :identity
     :index true or false
     :fulltext true or false
     :isComponent true or false (only for :ref types)
     :noHistory true or false"
  [conn name & options]
  (transact! conn (apply create-attribute-txn name options)))

(defn create-attributes!
  "Applies create-attribute! on each of it's arguments."
  [conn & args-seq]
  (doseq [args args-seq]
    (apply create-attribute! conn args)))

(defn create-entity-txn
  "Returns the txn data to create an entity possessing the specified attributes."
  [conn partition attrs]
  (vector (assoc attrs :db/id (d/tempid partition))))

; TODO check for a unique attribute, retrieve the entity and return it
(defn create-entity!
  "Issue a transaction to create an entity with the specified attributes."
  [conn partition attrs]
  (transact! conn (create-entity-txn conn partition attrs)))

(defn ensure-db
  "Given a Datomic database or connection, return a database."
  [db-or-conn]
  (if (isa? (class db-or-conn) datomic.db.Db) db-or-conn (db db-or-conn)))

(defn list-attributes
  "Returns a list of attributes in the specified namespaces,
   or all namespaces when none are specified.

   Thanks to Stu Halloway: https://gist.github.com/2560986"
  [db-or-conn & [re]]
  (let [db (ensure-db db-or-conn)
        re (or re #".")]
    (map first
         (let [rules '[[[attr-match ?re ?attr]
                        [:db.part/db :db.install/attribute ?e]
                        [?e :db/ident ?attr]
                        [(str ?attr) ?str]
                        [(re-find ?re ?str)]]]]
           (q '[:find ?attr
                :in $ % ?re
                :where (attr-match ?re ?attr)]
              db rules re)))))

(defn describe-attribute
  "Returns a map of facts about an attribute."
  [db-or-conn attr-id]
  (into {} (d/entity (ensure-db db-or-conn) attr-id)))

(defn describe-attributes
  "Returns a map of attributes, keyed by :db/ident with maps of facts as values.
   When called without a seq of attr-ids, includes all attributes."
  ([db-or-conn attr-ids]
     (let [db (ensure-db db-or-conn)
           describer (partial describe-attribute db)
           attr-maps (map describer attr-ids)]
       (zipmap (map :db/ident attr-maps) (map #(dissoc % :db/ident) attr-maps))))
  ([db-or-conn]
     (describe-attributes db-or-conn (list-attributes db-or-conn))))

(defn simplify-attribute-description
  "Removes Datomic namespacing from attribute key-value pairs to simplify reading."
  [attr-desc]
  (let [remap {:db/cardinality :cardinality
               :db/unique :unique
               :db/index :index
               :db/doc :doc
               :db/valueType :type
               :db/fulltext :fulltext
               :db/noHistory :no-history
               :db/isComponent :component

               :db.cardinality/one :one
               :db.cardinality/many :many
               :db.unique/unique :unique
               :db.unique/identity :identity
               :db.type/long :long
               :db.type/string :string
               :db.type/keyword :keyword
               :db.type/ref :ref
               :db.type/boolean :boolean
               :db.type/instant :instant}]
    (into {}
          (map (fn [[k v]]
                 [(get remap k k) (get remap v v)])
               attr-desc))))

(defn pprint-attribute
  "Outputs a pretty-printed description of an attribute (given a description)."
  [attr-desc]
  (pp/pprint (simplify-attribute-description attr-desc)))