#!/bin/bash

#exit if find error
# ============================================================================

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
function error() {
   SCRIPT="$0"           # script name
   LASTLINE="$1"         # line of error occurrence
   LASTERR="$2"          # error code
   echo "ERROR exit from ${SCRIPT} : line ${LASTLINE} with exit code ${LASTERR}"
   exit 1
}
trap 'error ${LINENO} ${?}' ERR

# ============================================================================

PKG_PATH=$1
BENCHMARK_WORKSPACE=$2
KYBOT_ACCOUNT=$3
KYBOT_PASSWD=$4
USER_CONFIG_PATH=$5

#WORK_SPACE=/var/lib/jenkins/workspace/kap-load-test
#cd ${WORK_SPACE}


rm -rf ${BENCHMARK_WORKSPACE}/kap-*
tar -zxvf ${PKG_PATH} -C ${BENCHMARK_WORKSPACE}

cd ${BENCHMARK_WORKSPACE}/kap-*/
export KYLIN_HOME=`pwd`
echo 'kylin home : ' ${KYLIN_HOME}
#prod setting
cd ${KYLIN_HOME}/conf
rm -f profile
ln -s profile_prod profile

#config override
cp ${USER_CONFIG_PATH}/conf/kylin.properties.override ${KYLIN_HOME}/conf/

#meta
${KYLIN_HOME}/bin/metastore.sh reset
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete true
${KYLIN_HOME}/bin/metastore.sh restore ${USER_CONFIG_PATH}/metadata/

#start kylin server

${KYLIN_HOME}/bin/kylin.sh start


echo "Kylin server start !"

echo "Start stress test"
cd ${BENCHMARK_WORKSPACE}
mvn clean install -DskipTests
mvn exec:java -Dexec.mainClass="io.kyligence.benchmark.loadtest.cli.BenchmarkCLI" -Dexec.args="${USER_CONFIG_PATH}/conf/testcase.properties"
echo "Stress test finish"

echo "Cleanup metadata"
# Tear down stage
${KYLIN_HOME}/bin/kylin.sh stop

echo "Finished!"

#upload kybot zip package
cd ${KYLIN_HOME}
bash ./kybot/kybot.sh 
kybot_pkg_parent_name=`ls kybot_dump/ | grep kybot`
kybot_pkg_name=`ls kybot_dump/kybot*/ | grep kybot`
echo ${kybot_pkg_name}
cd ../

python scripts/kybot-upload-client.py -s https://kybot.io -u ${KYBOT_ACCOUNT} -p ${KYBOT_PASSWD} -f ${KYLIN_HOME}/kybot_dump/${kybot_pkg_parent_name}/${kybot_pkg_name} 
