(ns vinzi.tools.vProject
  (:use	[clojure 
         [pprint :only [pprint pp]]]
        [clojure 
         [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools 
         [logging :only [error info trace debug warn]]])
  (:require [clojure
             [string :as str]
             [set :as set]
             [xml :as xml]
             [zip :as zip]]
            [clojure.java
             [io :as io]
             [shell :as sh]]
            [clojure.data.zip.xml :as zx]
            [vinzi.tools 
             [vExcept :as vExcept]
             [vFile :as vFile]]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Tools for handling clojure projects
;;     - migrate from pom2Proj (Tom Hickley)
;;     - parsing of classpaths
;;     -  etc
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ClassPathSep (if vFile/runningWindows ";" ":"))

(def RepoBase (vFile/filename "~" ".m2/repository/"))




(defmacro defproject 
  "macro used to read a lein project.clj file via read-str and translate it to a hashmap"
  [nme version & opts]
  (let [ver# (-> version (str/split #"\-"))]
  `{:projNme ~(name nme) 
    :version ~(first ver#)
    :classifier ~(second ver#)
    :params  (apply hash-map '~opts)}))

(defn read-lein-project [projFolder]
  (eval (read-string (slurp (vFile/filename projFolder "project.clj")))))

(defn get-main-jar-dependency 
  "Get the main jar of a project shaped as a dependency."
  [proj]
  {:pre [(map? proj)]}
  (vector (let [artifId (:projNme proj)
                groupId (->> (str/split artifId #"\.")
                          (drop-last)
                          (str/join "."))]
            (symbol (str groupId "/" artifId))) 
          (let [cls (:classifier proj) 
                ver (:version proj)]
            (if (seq cls) (str ver "-" cls) ver))))

(defn dependency-to-classpath 
  "Translate a dependency to a classpath part (relative path)"
  [pars]
  (let [[nme verCls] pars
        nmes (str nme)   ;; use string as name removes prefix
        nme (name nme)
        [groupId artifId] (str/split nmes #"/")
        artifId (if (seq artifId) artifId groupId)
        groupId (str/replace groupId #"\." "/")
        cpPart (str/join "/" (list groupId artifId verCls (str nme "-" verCls ".jar")))]
    cpPart))

  

(defn extract-classpath 
  "Extract a classpath from a project as read by read-lein-project.
   NOTE: this classpath only contains the dependencies shown in the project-file and does not dig
    into the jars for deeper/transitive dependencies."
  [proj]
  {:pre [(map? proj)]}
  (let [mainJar (get-main-jar-dependency proj)
        _ (println "mainJar="mainJar)
        cp (-> proj (:params) (:dependencies) (concat (list mainJar)))]
    (map dependency-to-classpath cp)))

(defn extract-lein-classpath 
  "Read the project.clj from 'projFolder and extract the classpath (including the project jar-file)."
  [projFolder]
  (extract-classpath (read-lein-project projFolder)))


(defn get-main-jar 
  "Extract the main jar from a leiningen project."
  [projFolder]
  (-> projFolder
    (read-lein-project )
    (get-main-jar-dependency )
    (dependency-to-classpath )))



(defn split-classpath 
  "Split a classpath in a series of classpath components."
  [cp]
  (println "retrieved classpath: " cp)
  (str/split cp (re-pattern ClassPathSep)))


(defn retrieve-proj-classpath [commArgs cleanOut projFolder]
  (sh/with-sh-dir (vFile/filename projFolder)
    (-> (apply sh/sh commArgs)
      (:out)
;;      (#(do (def lastCp %) %))
      (cleanOut)
      (#(do (def lastCp %) %))
      (split-classpath))))



;; See also extract-class-path-lein that extracts class-path directly from the project (does not need to run lein and correctly insertes project-jarfile.
(def retrieve-lein-classpath (partial retrieve-proj-classpath '("lein" "-o" "classpath") identity))
;; using lein -o, otherwise it might fail due to a lacking internet connection.

(def retrieve-mvn-classpath  (partial retrieve-proj-classpath 
                                      '("mvn" "org.apache.maven.plugins:maven-dependency-plugin:2.6:build-classpath")
                                      (fn [cp] (->> cp 
                                                 (str/split-lines) 
                                                 (remove #(.startsWith % "["))
                                                 (filter #(seq (str/trim %)))
                                                 (#(do
                                                     (when (> (count %) 1)
                                                       (debug "retrieved multiple class-path lines: " (with-out-str (doseq [s %] (println s)))))
                                                     %))
                                                 (first)))))

(defn retrieve-classpath [projFolder]
  (if (vFile/file-exists (vFile/filename projFolder "project.clj"))
    (let [lpf "(retrieve-classpath): "
          baseCp (vFile/filename (get (System/getenv) "HOME") ".m2/repository/")
          msg #(do (debug lpf %2 " interm.res: " %1) %1)
          cp (-> projFolder 
               (msg "retrieve folder")
               (retrieve-lein-classpath)  ;; returns a vector of classpaths
               ; (str/split #":")
               (msg "trim folders")
               ((partial map str/trim) )   ;; last item might have a \n
               (msg "filter jars")
               ((partial filter #(.endsWith % ".jar")) )
               (msg "remove base-classpath")
               ((partial map #(if (.startsWith % baseCp) (subs % (count baseCp)) %)) ))
          _ (debug lpf "get main jar")
          mj (get-main-jar projFolder)]
      (debug lpf " result= " (cons mj cp))
      (cons mj cp))
    (if (vFile/file-exists (vFile/filename projFolder "pom.xml"))
      (retrieve-mvn-classpath projFolder)
      (vExcept/throw-except "(retrieve-classpath): Couldn't find project.clj or pom.xml in folder: " projFolder))))


(defn copy-required-jars 
  "Copy all jars from the class-path, leaving out the source-folders. The modified path is returned."
  [cp jarFolder]
  {:pre [(sequential? cp) (string? jarFolder)]}
  (println "processing for jarFolder="jarFolder)
  (let [cp   (filter #(and (.startsWith % RepoBase) (.endsWith % ".jar")) cp)
        copy-jar (fn [fName]
                     (let [relDir (-> fName 
                                      (vFile/get-path-dir)
                                      (str/replace (re-pattern (str "^" RepoBase)) ""))
                           tarDir (vFile/filename (vFile/filename jarFolder relDir) "")
                           tarNme (vFile/filename tarDir (vFile/get-filename fName))]
                       (vFile/ensure-dir-exists tarDir)
                       (println "JarFolder=" jarFolder " and tarDir=" tarDir)
                       (println " copy " fName " to " tarNme)
                       (io/copy (io/file fName) (io/file tarNme)))
                     (println "filename: " fName " does not qualify as repository jar."))]
    (doseq [path cp]
      (println "handling path: " path)
      (copy-jar path))
    cp))


(defn copy-project-jars
  "Copy all jars to the jarFolder (will be created)."
  [projFolder jarFolder]
  (let [cp (retrieve-classpath projFolder)]
    (copy-required-jars cp jarFolder)))
                               
  
(defn make-win-project 
  "Generate a windows-batch-file a filled jar-folder (however, main project needs to be added manually)"
  [projFolder jarFolder main-class args]
  (let [cp (-> (retrieve-classpath projFolder)
             (copy-required-jars jarFolder))
        projNme (vFile/get-filename projFolder)
        wcp (str/join ";" (map #(-> % (str/replace (re-pattern (str "^" RepoBase)) "./")
                                  ((fn[x] (do (println "intermediate value: " x) x)))
                                  (str/replace #"/" "\\\\")) cp))
        cmd (str/join "\n" (list (str "REM running program " projNme " (" main-class ")")
                      "" 
                      (str "REM You still need to add the correct jar containing: " main-class " to the classpath ")
                       "REM \tand to the folder-structure!!"
                      (str "java -classpath " wcp " clojure.main -m " main-class  " " (str/join " " args))
                      ""))]
    (spit (vFile/filename jarFolder (str projNme ".bat")) cmd)))



(defn merge-classpaths [cp1 cp2 & cps]
  (let [merge2 (fn [cp1 cp2]
                 (let [cpaths (set cp1)]
                   (reduce #(if (cpaths %2) %1 (conj %1 %2)) (vec cp1) cp2))) 
        mrg (merge2 cp1 cp2)]
    (if (seq cps)
      (apply merge2 mrg cps)
      mrg)))

(defn classpath-str 
  "Turn a sequence of classpath-items into a string in the format of the current OS (windows or linux)"
  ([cp] (classpath-str "" cp)) 
  ([prefix cp]
    (str/join ClassPathSep (map #(vFile/filename prefix %) cp))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Author pom2proj is Tom Hickley
;;  CvK made a few fixes to let it work with my pom-files
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- text-attrs
  [loc ks]
  (map (fn [k]
         (zx/xml1-> loc k zx/text))
       ks))

(defn- attr-map
  [loc ks]
  (when loc
    (reduce (fn [m k]
              (if (and m k)
                (assoc m k (zx/xml1-> loc k zx/text))
                m))
            {} ks)))

(defn- map-by-attr
  [locs k-key v-key]
  (reduce (fn [m loc]
            (let [[k v] (text-attrs loc [k-key v-key])]
              (assoc m k v)))
          {} locs))

(defn- artifact-sym
  [{:keys [groupId artifactId]}]
  (if artifactId
    (symbol (if (= groupId artifactId)
              artifactId
              (str groupId "/" artifactId)))
    (symbol "no-artifact")))

(defn- exclusions
  [dep-loc]
  (map #(attr-map % [:groupId :artifactId])
       (zx/xml-> dep-loc :exclusions :exclusion)))

(defn- dependency
  [dep-loc]
  (let [dep (attr-map dep-loc [:groupId :artifactId :version])
        exclusions (exclusions dep-loc)]
    (if (empty? exclusions)
      dep
      (assoc dep :exclusions exclusions))))

(defn- dependencies
  [pom-zip]
  (zx/xml-> pom-zip :dependencies :dependency dependency))

(defn- parent
  [pom-zip]
  (attr-map (zx/xml1-> pom-zip :parent)
            [:groupId :artifactId :version :relativePath]))

(defn- project
  [pom-zip]
  (attr-map pom-zip
            [:groupId :artifactId :version :description :url]))

(defn- repositories
  [pom-zip]
  (map-by-attr (zx/xml-> pom-zip :repositories :repository)
               :id :url))

(defn- deploy-repositories
  [pom-zip]
  (if-let [dist (zx/xml1-> pom-zip :distributionManagement)]
    {:releases (zx/xml1-> dist :repository :url zx/text)
     :snapshots (zx/xml1-> dist :snapshotRepository :url zx/text)}
    {:releases "no distrib-mgt"
     :snapshots "no distrib-mgt"}))

(defn- build
  [pom-zip]
  (let [bld (zx/xml1-> pom-zip :build)]
    {:source-paths [(zx/xml1-> bld :sourceDirectory zx/text)]
     :test-paths [(zx/xml1-> bld :testSourceDirectory zx/text)]
     :resource-paths (into [] (zx/xml-> bld :resources :resource :directory zx/text))}))

(defn- lein-project-info
  [xz]
  (assoc (merge (project xz) (build xz))
         :dependencies (dependencies xz)
         :repositories (repositories xz)
         :deploy-repositories (deploy-repositories xz)
         :parent (parent xz)))

(defn- format-dependencies
  [deps]
  (into [] (map (fn [dep]
                  (let [d [(artifact-sym dep) (:version dep)]
                        ex (:exclusions dep)]
                    (if ex
                      (conj d :exclusions
                            (into [] (map artifact-sym ex)))
                      d)))
                deps)))

(defn- format-parent
  [parent]
  (let [rel (:relativePath parent)
        p [(artifact-sym parent)]]
    (if rel
      (conj p :relative-path rel)
      p)))

(defn- format-lein-project
  [{:keys [dependencies repositories parent] :as project}]
  (let [info (dissoc project :groupId :artifactId :version)
        info (assoc info
                    :dependencies (format-dependencies dependencies)
                    :parent (format-parent parent))
        info (into {} (filter (fn [[k v]] v) info))
        pf `(~'defproject ~(artifact-sym project) ~(:version project)
                          ~@(mapcat (fn [[k v]] [k v]) info))]
    pf))

(defn process-pom
  "The main function that requires two paths that both end in a file-separator"
  ([from to]
    (let [xz (zip/xml-zip (xml/parse (io/file (vFile/filename from "pom.xml"))))
          project-info (lein-project-info xz)
          f (format-lein-project project-info)]
      (spit (vFile/filename to "project.clj") (with-out-str (pprint f)))
      (pprint f)))
  ([from]
    (process-pom from from)))
