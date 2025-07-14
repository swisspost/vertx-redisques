#!/bin/bash
set -ev
git fetch
git reset --hard
mvn -B -Prelease gitflow:release-start -DskipTests
mvn -B -Prelease --batch-mode deploy -DskipTests
mvn -B -Prelease gitflow:release-finish -DskipTests
rc=$?
if [ $rc -eq 0 ]
then
    echo 'Release done, will push'
  exit 0
fi
echo 'Release failed'
exit $rc
