#!/bin/bash
set -ev
git fetch
git reset --hard
mvn mvn -B -Prelease jgitflow:release-start jgitflow:release-finish
rc=$?
if [ $rc -eq 0 ]
then
    echo 'Release done, will push'
  exit 0
fi
echo 'Release failed'
exit $rc
