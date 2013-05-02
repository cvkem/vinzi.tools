(ns vinzi.tools.vCacheXml
  (:use	[clojure pprint]
        [clojure.tools logging])
  (:require [clojure
             [xml :as xml]
             [string :as str]]
            [vinzi.data.zip :as vZip]
            [vinzi.tools
             [vFile :as vFile]
             [vExcept :as vExcept]]))


(defn create-xml-cache 
  [xml-init xml-clear]
  (let [xmlFileCache (atom {})]
    (letfn [(extend-xml-filepath 
              ;;Path should be extended. When used as a standalone application
              ;;this function extends to the current directory. 
              ;;NOTE: when used as a pentaho-plugin the plugin adaptor should take care that fName
              ;;contains a full path relative to root (no guarantees about the current folder). 
              [fName]
              (if (vFile/full-path? fName)
                fName
                (vFile/filename (vFile/get-current-dir) fName)))
            (read-xml-file  
              ;;Read a xml-file and parse the xml
              [fName]
              (let [lpf "(read-xml-file): "
                    read-xml (fn [fName]
                               (try 
                                 (xml/parse fName)
                                 (catch Exception e
                                   (let [prefix (str lpf "Could NOT open xml-file: " fName)
                                         logMsg (str prefix "\nException: " (.getMessage e))]
                                     (error logMsg)
                                     (binding [*out* *err*]
                                       (println prefix)) ;; file-info (duplic?)
                                     (throw e)))))]  ;; re-throw same exception
                (let [xml  (read-xml fName)]
                  xml)))
            (get-xml-file 
              ;; Try to read the config-file from the request from the internal cache. 
              ;; If it is not available read it from disc and cache it and apply the initialization.
              [fName]
              (let [lpf "(get-xml-file): "
                    fName (extend-xml-filepath fName)
                    ;; key should be based on the full-filename
                    keyfName (keyword fName)]
                (if-let [cfg (keyfName @xmlFileCache)]
                  (do
                    (trace "retrieved configuration " fName " from cache")
                    cfg)  ;; file was already in cache
                  (if-let [cfg (read-xml-file fName)]
                    (let [topLevelText (remove map? (:content cfg))]
                      (when (seq topLevelText)
                        (vExcept/throw-except lpf "Top-level of xml contains text (tags missing?): " (str/join "\n" topLevelText)))
                      (trace "Loaded a new configuration from file: " fName
                             ;;                  "\nContents: " (with-out-str (pprint cfg))
                             "\nAdding contents to the internal xml-cache.")
                      (try
                        (swap! xmlFileCache assoc keyfName cfg)
                        ;; run initialization after adding it to cache, as initialization code might assume
                        ;; that the file exist (example global section of cdp might execute functions from its implementation)
                        (when (fn? xml-init)
                          (xml-init fName cfg))
                        (catch Throwable e
                          ;; exception during initialization, so remove it and rethrow
                          (swap! xmlFileCache dissoc keyfName)
                          (throw e)))
                      cfg)
                    (error lpf "Could not read " fName " (no valid xml?)")))))
            (list-xml-file-entries []
                              (keys @xmlFileCache))
            (update-xml-entry 
              ;;Update a xml-entry in memory to store additional information or modifications.
              ;;NOTE: the version on file is not updated.
              [fName newEntry]
              {:pre [(not (vZip/is-zipper? newEntry))]} ;; in memory we store xml.
              (let [lpf "(update-xml-entry): "
                    _   (trace lpf "the fName to be keywordized is: " fName)
                    keyfName (-> fName
                               (extend-xml-filepath)
                               (keyword))]
                (trace lpf "The cache-entry is: " (keyfName @xmlFileCache))
                (if-let [current (keyfName @xmlFileCache)]
                  (swap! xmlFileCache (fn [cc] (assoc cc keyfName newEntry)))
                  (vExcept/throw-except lpf "Entry with name " keyfName "does not exist (No update performed),"
                                        "\n\t Valid file-entries are: " (str/join "\n\t\t" (list-xml-file-entries)) "\n"))))
            (clear-xml-cache 
              ;; Clear the xml-cache.
              []
              (info "(clear-cache): Clearing the xml-file cache")
              ;; TODO: add exception handling
              (let [xmls @xmlFileCache]
                (doseq [xml (vals xmls)]
                  (when (fn? xml-clear)
                    (xml-clear xml))
             ))
              (swap! xmlFileCache (fn [_] {}))
              ;;{:plain "succes: xml-cache cleared"}
              (str "<html><h2>xml/clear-cache</h2><p>The xml-cache was cleared at " (java.util.Date.) "</p></html>"))]
      {:get-xml-file      get-xml-file
       :update-xml-entry  update-xml-entry
       :clear-xml-cache   clear-xml-cache
       :list-xml-file-entries list-xml-file-entries
       }
            
            )))	    