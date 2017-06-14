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

#args
KYLIN_PKG_PATH=$1
KYLIN_INSTANCE_HOME=kylin-instance-home
CONFIG_DIR_PATH=workload
PROJECT_BASE_DIR=`pwd`
SETENV_PATH=/root/load-test-workloads

echo "running start_kylin.sh"
echo "KYLIN_PACKAGE_PATH:${KYLIN_PKG_PATH}"
echo "PROJECT_BASE_DIR:${PROJECT_BASE_DIR}"

#release tar
rm -rf ${KYLIN_INSTANCE_HOME}
mkdir ${KYLIN_INSTANCE_HOME}
tar -zxvf ${KYLIN_PKG_PATH} -C ${KYLIN_INSTANCE_HOME}

#set kylin home
cd ${KYLIN_INSTANCE_HOME}/kap-*/
export KYLIN_HOME=`pwd`
echo 'kylin home : ' ${KYLIN_HOME}

#skip license check
rm -f ${KYLIN_HOME}/bin/check-license.sh

#modify jvm setting
cp -f ${SETENV_PATH}/setenv.sh ${KYLIN_HOME}/bin/

#prod setting
cd ${KYLIN_HOME}/conf
rm -f profile
ln -s profile_prod profile

#config override
cp ${PROJECT_BASE_DIR}/${CONFIG_DIR_PATH}/conf/kylin.properties.override ${KYLIN_HOME}/conf/

#reload metadata
${KYLIN_HOME}/bin/metastore.sh reset
${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.storage.KapStorageCleanupCLI
${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.storage.KapStorageCleanupCLI --delete true
${KYLIN_HOME}/bin/metastore.sh restore ${PROJECT_BASE_DIR}/${CONFIG_DIR_PATH}/metadata/

#start kylin server
${KYLIN_HOME}/bin/kylin.sh start

#sleep a while waiting kylin server start
echo "sleep a while waiting kylin server start"
sleep 30s

cd ${PROJECT_BASE_DIR}

echo "Kylin server start !"
