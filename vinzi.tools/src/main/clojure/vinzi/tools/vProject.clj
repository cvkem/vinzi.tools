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



(def retrieve-lein-classpath (partial retrieve-proj-classpath '("lein" "classpath") identity))
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
    (retrieve-lein-classpath projFolder)
    (if (vFile/file-exists (vFile/filename projFolder "pom.xml"))
      (retrieve-mvn-classpath projFolder)
      (vExcept/throw-except "(retrieve-classpath): Couldn't find project.clj or pom.xml in folder: " projFolder))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Author pom2proj is Tom Hickley
;;  made a few fixes to let it work with my pom-files
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
    (let [xz (zip/xml-zip (xml/parse (io/file (str from "pom.xml"))))
          project-info (lein-project-info xz)
          f (format-lein-project project-info)]
      (spit (str to "project.clj") (with-out-str (pprint f)))
      (pprint f)))
  ([from]
    (process-pom from from)))
