#!/bin/bash
REF=$1
if [ -z ${REF} ]; then REF=master; fi
gcovr --filter='.*src/realm.*' -x >gcovr.xml
diff-cover gcovr.xml --compare-branch=${REF} --html-report coverage.html
firefox coverage.html
