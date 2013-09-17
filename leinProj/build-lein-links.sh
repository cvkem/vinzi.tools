#!/bin/sh

echo  Build the links to the source-code and test-code. 
echo The links refer  to the mvn-project structure included in this repo.

PROJECT="vinzi.tools"

ln -s ../${PROJECT}/src/main/clojure src
ln -s ../${PROJECT}/src/test/clojure test
ln -s ../${PROJECT}/src/main/resources resources

echo  Result: 
ls -al

