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

#get args
BENCHMARK_WORKSPACE=$1
KYLIN_PKG_PATH=$2
CONFIG_DIR_PATH=$3

echo "running start_kylin.sh"
echo "BENCHMARK_WORKSPACE:${BENCHMARK_WORKSPACE}"
echo "KYLIN_PACKAGE_PATH:${KYLIN_PKG_PATH}"
echo "CONFIG_DIR_PATH:${CONFIG_DIR_PATH}"

#release tar
rm -rf ${BENCHMARK_WORKSPACE}/kap-*
tar -zxvf ${KYLIN_PKG_PATH} -C ${BENCHMARK_WORKSPACE}

#set kylin home
cd ${BENCHMARK_WORKSPACE}/kap-*/
export KYLIN_HOME=`pwd`
echo 'kylin home : ' ${KYLIN_HOME}

#prod setting
cd ${KYLIN_HOME}/conf
rm -f profile
ln -s profile_prod profile

#config override
cp ${CONFIG_DIR_PATH}/conf/kylin.properties.override ${KYLIN_HOME}/conf/


#reload metadata
${KYLIN_HOME}/bin/metastore.sh reset
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete true
${KYLIN_HOME}/bin/metastore.sh restore ${CONFIG_DIR_PATH}/metadata/

#start kylin server

${KYLIN_HOME}/bin/kylin.sh start




echo "Kylin server start !"
