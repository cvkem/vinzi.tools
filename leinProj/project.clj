(defproject
 vinzi/vinzi.tools
 "0.2.0-SNAPSHOT"
 :dependencies
 [[org.clojure/clojure "1.5.1"]
    ;;  temporary add it. Should only be called on the repl
  [com.ambrosebs/dynalint "0.1.1"]
  [org.clojure/java.jdbc "0.2.3"]
  [org.clojure/tools.logging "0.2.6"]
  [org.slf4j/slf4j-api "1.6.5"]
  [ch.qos.logback/logback-core "1.0.6"]
  [ch.qos.logback/logback-classic "1.0.6"]
  [org.clojure/data.csv "0.1.2"]
  [postgresql "9.1-901-1.jdbc4"]
  [vinzi/vinzi.data "0.1.0-SNAPSHOT"]
  [org.apache.poi/poi "3.9"]
  [org.apache.poi/poi-ooxml "3.9"]]
 :deploy-repositories
 {:snapshots "no distrib-mgt", :releases "no distrib-mgt"}
 :source-paths ["src"]
 :test-paths ["test"]
 :resource-paths ["resources"]
 :repositories
 {"Clojars" "http://clojars.org/repo",
  "Clojure Releases" "http://build.clojure.org/releases"}
:pom-plugins [[com.theoryinpractise/clojure-maven-plugin "1.3.13"
                                ;; this section is optional, values have the same syntax as pom-addition
               [:sourceDirectories [:sourceDirectory "src"]]
               [:extensions "true"]
               [:executions ([:execution [:id "clojure-compile"]
                                        [:phase "compile"]
                                        [:goals [:goal "compile"]]])]]]
 :description
 "Set of (generic) auxiliary functions to support vinzi libraries, programs and tools")
