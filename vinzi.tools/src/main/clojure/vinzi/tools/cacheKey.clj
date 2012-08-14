(ns vinzi.tools.cacheKey
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [string :as str]]
            [clojure.java
             [jdbc :as sql]]
            [vinzi.tools.vDateTime :as vDate]
            ;;	zookeeper.data)
            )
;;  (:import [java.nio ByteBuffer])
)

;; This name-space is contains the implementation of a global cache for
;; the anchor-model and the key-generating routines.
;; Currently the cache is implemented as a global cache.
;;  TODO: add better key-function (minimize key-size and maximize entropy)
;;  TODO: Protect against multiple simultaneous writes to same cache.
;;  TODO: Switch to REDIS server

;; use a binary compactified key (beter hash results)
(def BinaryCompactKeys false)


;; container for all caches (or for a single global cache)
;; each cache is represented by an atom, such that caches can be updated
(def CacheIdentifiers :cache-identifiers)
(def NextCacheId :next-cache-id)
(def GlobalCache (atom {}))

;; Declarations of the (primitive) cache-functions that will be used to
;; initialize the caching-system too.
(declare create-cache)
(declare get-cache)
(declare set-cache-entry)

(defn- initialize []
  (create-cache CacheIdentifiers)
  (let [cache (get-cache CacheIdentifiers)]
    (trace "During initialize obtained cache: " cache)
    (set-cache-entry cache NextCacheId 1)
    ;; add own name to cache
    (set-cache-entry cache CacheIdentifiers CacheIdentifiers)))

(defn- create-cache "Create a new cache with 'cacheId'."[cacheId]
  (let [lpf (str "(create-cache " cacheId ")")
	newCache (atom {})]
    (trace lpf "Generating new cache")
    (swap! GlobalCache (fn [gc] (assoc gc cacheId newCache)))))

(defn- get-cache "Returns a vector containing ['cacheId' 'cache'] if the cache exists, or nil otherwise." [cacheId]
  (when-let [cache (get @GlobalCache cacheId)]
    [cacheId cache]))

(defn set-cache-entry "Set the cache-entry with key 'k' to value 'v'." [cache & kvs]
  (let [lpf "(set-cache-entry): "]
    (when (not (even? (count kvs)))
      (error lpf "Expected key-value pairs. Received more keys than values.")
      (assert (even? (count kvs))))
    (let [ch (second cache)
	  newEntries (apply hash-map kvs)]
      (swap! ch (fn [c] (into c newEntries))))))


(defn get-cache-entry "Lookup a cache entry in 'cache'." [cache k]
  (let [v (get @(second cache) k)]
    v))

(defn inc-get-cache-entry "Return the current value of a counter and increment it afterwards." [cache k]
  (let [v (get-cache-entry cache k)]
    (println "received value " v " of type " (type v))
    (set-cache-entry cache k (inc v))
    v))

(defn init-cache "Return the id of an existing cache or a new cache with name nme. Names of caches are stored in 'CacheIdentifiers' cache." [nme]
  (let [lpf (str "(init-cache " nme "): ")
	idCache (get-cache CacheIdentifiers)
	cid (if-let [id (get-cache-entry idCache nme)]
	      (do
		(trace lpf " Returning existing cache-id=" id)
		id)
	      (let [id (inc-get-cache-entry idCache NextCacheId)]
		(create-cache id)
		(set-cache-entry idCache id nme)
		id))]
    (trace lpf " retrieving the cacheDescriptor for id=" cid)
    (get-cache cid)))





;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   implementation of key-generation for string-based keys
;;          - gen-compound-key: implemented by gen-compound-key-bin
;;          - unpack-key:  implemented by unpack-key-bin
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn- packed-binary-descr [x]
  (throw (Exception. "(packed-binary-descr) excluded from code"))
  (comment  ;; temporarily excluded

            (let [b (to-bytes x)
	bCnt (count b)
	fnz (loop [ks (range bCnt)]
	      (if (seq ks)
		(let [k (first ks)]
		  (if (= (get b k) 0)
		    (recur (next ks))
		    k)) ;; found first non zero value
		bCnt))]  ;; all field 0..(cnt-1) are zero
    ;; return vector with: b=binary repres, fnz=firstNonZero cnt=byteCount
    [b fnz bCnt])))

(defn- pack-binary "The 'bs' is a series of binary-descriptors, as produced by pack-binary-descr. Theis functions returns a single byte-array containing all the values and prefixed by a header that will be used to unpack it." [bs]

    (throw (Exception. "(pack-binary) excluded from code"))
  (comment  ;; temporarily excluded

  (let [;; compute the size of the header (contains an even number
	;; of 4-bit data-blocks.
	headSize (int (/ (count bs) 2))
	headSize (if (odd? (count bs)) (inc headSize) headSize)
	;;	  _ (trace "bs=" bs)
	;; compute the length
	len (map #(- (get % 2) (get % 1)) bs)
	;;	  _ (trace "len=" len)
	;; compute the actuals start-positions in the byte-array based
	;; on size of the header and the preceding fields
	spf (fn [sps x] (let [sp (last sps)]
			  (conj sps (+ sp x))))
	startPos (reduce spf [headSize] len)
	;;	  _ (trace "startPos=" startPos)
	;; as sequence is prefixed by headSize the last correspond
	;; to the size of the sequence
	size (last startPos)
	;;	  _ (trace "size=" size)
	bbuf (ByteBuffer/allocateDirect size)]
    (letfn [(write-header
	     [len]
	     ;; the header is a descriptor of the data containing ...
	     (if (seq len)
	       (let [;;b (byte (+ (* (first len) 16) (second len)))
		     b1 (bit-shift-left (first len) 4)
		     b2 (bit-and (second len) 0xf)
		     b  (bit-or b1 b2)
		     b  (byte (if (> b 127) (- b 256) b))]
		 ;;	       (trace "computed combined headerbyte of " b
		 ;;		      "of type " (type b))
		 ;;	       (put-byte bbuf b)
		 (.put bbuf b)
		 (recur (drop 2 len)))))
	    (write-data
	     [bs len]
	     (when (seq bs)
	       (let [bt (ffirst bs)
		     sp (second (first bs))
		     l (first len)]
		 ;;	       (trace " copy data from start=" sp
		 ;;		      " for len= " l " from bytes " bt)
		 (.put bbuf bt sp l)
		 (recur (rest bs) (rest len)))))]
      (write-header (if (even? (count len)) len
			     (conj (vec len) 0))) ;; ensure even count
      (write-data bs len)
      (let [bb  (make-array Byte/TYPE size)] 
	(.clear bbuf)
	(.get bbuf bb 0 size)
	bb)))))

(defn gen-compound-key-bin "Generate a (binary) key for hash-map 'item' by concatenating the items of the keylist." [item keyList]
  ;; current version uses string concatenation and a separator.
  ;; The next version will create compact binary keys.
  (let [lpf "(gen-compound-key): "
	kVals (map #(% item) keyList)
	bVals (map packed-binary-descr kVals)
	k  (pack-binary bVals)]
;;    (trace lpf "item: " item " with keyList " keyList
;;	   "\nresulted in kVals=" kVals
;;	   "\ngenerated key of size " (count k) " key=" k)
    k))

(defn unpack-key-bin "This routine the the reverse operation of gen-compound-key. It splits the key in its constituent fields and translates these fields to the appropriate types again." [bKey fldTypes]
  (throw (Exception. "(unpack-key-bin) excluded from code"))
  (comment  ;; temporarily excluded

  (let [lpf "(unpack-key): "
	fCnt (count fldTypes)
	headSize (int (/ fCnt 2))
	headSize (if (odd? fCnt) (inc headSize) headSize)]
    (letfn [(red-unpack-len-byte
	     [cumm b]
;;	     (trace lpf "unpack-len-byte: " b)
	     (let [b  (if (< b 0) (+ b 256) b)  ;; make unsigned
		   b1 (bit-shift-right b 4) 
		   b2 (bit-and b 0xf)]
;;	       (trace lpf " retrieved: b1=" b1 " and b2=" b2)
	       (conj cumm b1 b2)))
	    (red-get-start-pos
	     [cumm l]
	     (conj cumm (+ l (last cumm))))
	    (get-byteBuffer
	     [bSize startPos len]
	     ;; get a bytebuffer of 'bSize' contains bytes
	     ;;    'startPos' .. 'startPos' + 'len' - 1
	     (let [bb  (ByteBuffer/allocate bSize)
		   prefix (int (- bSize len))
		   endPos (int (+ startPos len))]
;;	       (trace lpf "bSize=" bSize " start=" startPos " len=" len)
	       (loop [prefix prefix]
		 (when (> prefix 0)
;;		   (trace lpf "put-prefix: " prefix)
		   (.put bb (byte 0))
		   (recur (dec prefix))))
;;	       (trace lpf "added the prefix")
	       (loop [pos    startPos]
		 (when (< pos endPos)
;;		   (trace lpf " pos " pos " retrieved value: " (get bKey pos))
		   (.put bb (byte (get bKey pos)))
		   (recur (inc pos))))
	       (.clear bb)
	       ))
	    (get-byteBuffer-long
	     [size startPos len]
	     ;; get a bytebuffer of 'size' contains bytes
	     ;;    'startPos' .. 'startPos' + 'len' - 1
	     (let [bb  (ByteBuffer/allocate size)
		   prefix (int (- size len))
		   endPos (int (+ startPos len))]
;;	       (trace lpf "size=" size " start=" startPos " len=" len)
	       (loop [prefix prefix]
		 (when (> prefix 0)
;;		   (trace lpf "put-prefix: " prefix)
		   (.put bb (byte 0))
		   (recur (dec prefix))))
;;	       (trace lpf "added the prefix")
	       (loop [pos    startPos]
		 (when (< pos endPos)
;;		   (trace lpf " pos " pos " retrieved value: " (get bKey pos))
		   (.put bb (byte (get bKey pos)))
		   (recur (inc pos))))
	       (.clear bb)
	       (.getLong bb)))
	    (get-byteBuffer-double
	     [size startPos len]
	     ;; get a bytebuffer of 'size' contains bytes
	     ;;    'startPos' .. 'startPos' + 'len' - 1
	     (let [bb  (ByteBuffer/allocate size)
		   prefix (int (- size len))
		   endPos (int (+ startPos len))]
;;	       (trace lpf "size=" size " start=" startPos " len=" len)
	       (loop [prefix prefix]
		 (when (> prefix 0)
;;		   (trace lpf "put-prefix: " prefix)
		   (.put bb (byte 0))
		   (recur (dec prefix))))
;;	       (trace lpf "added the prefix")
	       (loop [pos    startPos]
		 (when (< pos endPos)
;;		   (trace lpf " pos " pos " retrieved value: " (get bKey pos))
		   (.put bb (byte (get bKey pos)))
		   (recur (inc pos))))
	       (.clear bb)
	       (.getDouble bb)))
	    (get-byteBuffer-int
	     [size startPos len]
	     ;; get a bytebuffer of 'size' contains bytes
	     ;;    'startPos' .. 'startPos' + 'len' - 1
	     (let [bb  (ByteBuffer/allocate size)
		   prefix (int (- size len))
		   endPos (int (+ startPos len))]
;;	       (trace lpf "size=" size " start=" startPos " len=" len)
	       (loop [prefix prefix]
		 (when (> prefix 0)
;;		   (trace lpf "put-prefix: " prefix)
		   (.put bb (byte 0))
		   (recur (dec prefix))))
;;	       (trace lpf "added the prefix")
	       (loop [pos    startPos]
		 (when (< pos endPos)
;;		   (trace lpf " pos " pos " retrieved value: " (get bKey pos))
		   (.put bb (byte (get bKey pos)))
		   (recur (inc pos))))
	       (.clear bb)
	       (.getDouble bb)))
	    (get-value
	     [tpe startPos len]
	     (case tpe
;		   "CHAR" (str x) 
;;		   "LONG" (.getLong (get-byteBuffer 8 startPos len)) 
		   "LONG" (get-byteBuffer-long 8 startPos len) 
;;		   "DOUBLE" (.getDouble (get-byteBuffer 8 startPos len))
;;  moved .getDouble into function to get rit of reflection issue		   
		   "DOUBLE" (get-byteBuffer-double 8 startPos len)
;		   "DATE" (vDate/str-to-sql-date x)
		   "INTEGER" (get-byteBuffer-int 4 startPos len)
;		   "FLOAT" (Float/parseFloat x) 
		   ))]
    (let [len (reduce red-unpack-len-byte [] (take headSize bKey))
	  startPos (reduce red-get-start-pos [headSize] len)
	  values (map get-value fldTypes startPos len)]
;;      (trace lpf " The lengths are: " len)
;;      (trace lpf "The start-positions are: " startPos)
;;      (trace lpf " The values are: " values)
      values)))))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   implementation of key-generation for string-based keys
;;          - gen-compound-key: implemented by gen-compound-key-str
;;          - unpack-key:  implemented by unpack-key-str
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def keySep "&|")
(def keySepRe (re-pattern #"&\|"))

(defn gen-compound-key-str "Generate a (binary) key for hash-map 'item' by concatenating the items of the keylist." [item keyList]
  ;; current version uses string concatenation and a separator.
      ;; The next version will create compact binary keys.
  (let [lpf "(gen-compound-key): "
	kVals (map #(% item) keyList)
	k (str/join keySep (map str kVals))]
;;    (trace lpf "item: " item " with keyList " keyList
;;	   "\nresulted in kVals=" kVals
;;	   "\ngenerated key of size " (count k) " key=" k)
    k))

(defn unpack-key-str "This routine the the reverse operation of gen-compound-key. It splits the key in its constituent fields and translates these fields to the appropriate types again.
However, the keys are returned as a list (and not as a map)!." [k fldTypes]
  (let [lpf "(unpack-key): "
	;; GS: (str/split "&|" #"&\|") => nil
	;; last item may not appear in items
	items (str/split k keySepRe)
	items (if (= (count items) (count fldTypes))
		items
		(concat items [""]))
	_ (when (not= (count items) (count fldTypes))
	    (debug lpf "Need equal numbers of items and fldTypes" items fldTypes))
	translate (fn [x tp]
		    (when (not= x "")
		      (case tp
			    "CHAR" (str x) 
			    "LONG" (Long/parseLong x) 
			    "DOUBLE" (Double/parseDouble x)
			    "double precision" (Double/parseDouble x)
			    "DATE" (vDate/str-to-sql-date x)
			    "INTEGER" (Integer/parseInt x)
			    "FLOAT" (Float/parseFloat x) 
			    )))]
    (map translate items fldTypes)))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Select the default implementation of
;;          - gen-compound-key
;;          - unpack-key
;; the specific implementations will also be available via direct calls.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def gen-compound-key (if BinaryCompactKeys
			gen-compound-key-bin
			gen-compound-key-str))

(def unpack-key (if BinaryCompactKeys
		  unpack-key-bin
		  unpack-key-str))



;(comment   ;;; test/demonstration of key handling
  
  (defn test-str-keys []
    (let [testRec {:int 10 :dbl 20.1}
	  strKey  (gen-compound-key-str testRec '(:int :dbl))
	  unpackedKey (unpack-key-str strKey '("LONG" "DOUBLE"))]
      (println "testRec = " testRec)
      (println "generates key = " strKey)
      (println "unpack of the key produces: " unpackedKey)))

  
  (defn test-bin-keys []
    (let [testRec {:int 10 :dbl 20.1}
	  binKey  (gen-compound-key-bin testRec '(:int :dbl))
	  unpackedKey (unpack-key-bin binKey '("LONG" "DOUBLE"))]
      (println "testRec = " testRec)
      (println "generates key = " binKey
	       "  (" (apply str (interpose " " binKey))")")
      
      (println "unpack of the key produces: " unpackedKey)))

  (defn test-bin-keys2 []
    (let [testRec {:int 10 :dbl 20.1}
	  binKey  (gen-compound-key-bin testRec '(:dbl :int))
	  unpackedKey (unpack-key-bin binKey '("DOUBLE" "LONG"))]
      (println "testRec = " testRec)
      (println "generates key = " binKey
	       "  (" (apply str (interpose " " binKey))")")
      
      (println "unpack of the key produces: " unpackedKey)))

(defn test-speed-keys []
  (let [testRec {:int 10 :dbl 20.1}
	testKeys '(:int :dbl)
	testTps  '("LONG" "DOUBLE")
	bKey    (gen-compound-key-bin testRec testKeys)
	sKey    (gen-compound-key-str testRec testKeys)
	numTest  10000]
    (println "Run test for generate binary key")
    (time (loop [i (int numTest)]
	    (when (> i 0)
	      (gen-compound-key-bin testRec testKeys)
	      (recur (dec i)))))
    (println "Run test for generate string key")
    (time (loop [i numTest]
	    (when (> i 0)
	      (gen-compound-key-str testRec testKeys)
	      (recur (dec i)))))
    (println "Run test for unpack bin key")
    (time (loop [i numTest]
	    (when (> i 0)
	      (unpack-key-bin bKey testTps)
	      (recur (dec i)))))
    (println "Run test for unpack string key")
    (time (loop [i (int numTest)]
	    (when (> i 0)
	      (unpack-key-str sKey testTps)
	      (recur (dec i)))))


    (println "\n\n New generate tests")
    (let [res (time (doall
		  (map #(list % (gen-compound-key-bin testRec testKeys))
			 (range numTest))))]
      (println "binary returned: " (count res) " items"))

    (let [res (time (doall
		  (map #(list % (gen-compound-key-str testRec testKeys))
			 (range numTest))))]
      (println "string returned: " (count res) " items"))

    (println "\n\n New test unpacking of keys")
    (let [res (time (doall
		     (reduce (fn [a b]
   ;;			       (println "reduce a=" a " b=" b)
;; NOTE: zodra je de doall uit de inner-loop haalt (volgende code-regel) dan crashed de functie met een stack-overflow  ???			       
			       (doall (map + a (second b))))
			     '(0 0.0)
			(map #(list % (unpack-key-bin bKey testTps))
			 (range numTest)))))]
      (println "binary returned: " (count res) " items"))


    (let [res (time (doall
		     (reduce (fn [a b]
			       (doall (map + a (second b))))
				  '(0 0.0)
			     (map #(list % (unpack-key-str sKey testTps))
			 (range numTest)))))]
      (println "string returned: " (count res) " items"))

    
    ))
  
 ; )






;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Inspect the cache system
;;

(defn reverse-map [m]
  ;; code-sniplet obtained from datalog (Rich Hickely)	 
  (into {} (map (fn [[k v]] [v k]) m)))

(defn get-cacheIds "Get all cache-ids." []
  (sort-by str (keys @GlobalCache)))

(defn get-cache-name "Get the symbolic name of a cache with 'cacheId'." [cacheId]
  (let [lpf (str "(get-cache-name " cacheId ")")
	[_ idCache] (get-cache CacheIdentifiers)
;;	_ (trace lpf " idCache contains: " @idCache)
;;	rIdCache (reverse-map @idCache)
;;	_ (trace lpf "  reverse map: " rIdCache)
	nme (get @idCache cacheId)]
    (trace lpf "CacheId=" cacheId " --> name=" nme)
    nme))


(defn show-cache "Dumps a complete cache via pprint." [cacheId]
  (let [[_ cache] (get-cache cacheId)
	nme (get-cache-name cacheId)]
    (println "\nShowing cache with id: " cacheId " and name " nme)
    (pprint @cache)))

(defn show-all-caches "Dumps all caches via pprint." []
  (doseq [cid (get-cacheIds)]
    (show-cache cid)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; initialize the cache system
;;
(initialize)


