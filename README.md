# dat-o-matic

A simple toolkit for working with [Datomic](http://datomic.com)

## Usage

```clojure
(use 'dat-o-matic.core)
    
(def conn (temp-conn))

(create-attribute! conn :example/color :type :string :doc "An entity's color")
(create-attribute! conn :example/size :type :string :doc "An entity's size")
(create-attribute! conn :example/stock :type :long :doc "How many of this entity are available")
(create-attribute! conn :example/popular :type :boolean :doc "An entity's size")

;; or

(create-attributes! conn 
                    [:example/color :type :string :doc "An entity's color"]
                    [:example/size :type :string :doc "An entity's size"]
                    [:example/stock :type :long :doc "How many of this entity are available"]
                    [:example/popular :type :boolean :doc "An entity's size"])

(list-attributes conn #"example")
(describe-attribute conn :example/color)

(create-partition! conn :my-data)

(safe-transact! conn '[{:db/id #db/id[:my-data]
                        :example/color "black"
                        :example/size "XS"
                        :example/stock 42
                        :example/popular true}])

(create-entity! conn :my-data 
                {:example/color "black" 
                 :example/size "XS"
                 :example/stock 42
                 :example/popular true})
```

## License

Copyright (C) 2012 deeperbydesign, inc.

Distributed under the Eclipse Public License, the same as Clojure.
