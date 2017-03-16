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
PROJECT_BASE_DIR=`pwd`
CONFIG_DIR_PATH=workload
KYLIN_INSTANCE_HOME=kylin-instance-home


#get kybot config
source ${PROJECT_BASE_DIR}/${CONFIG_DIR_PATH}/conf/kybot-config.sh

#set kylin home
cd ${KYLIN_INSTANCE_HOME}/apache-*/
export KYLIN_HOME=`pwd`
echo 'kylin home : ' ${KYLIN_HOME}

# Tear down stage
${KYLIN_HOME}/bin/kylin.sh stop
echo 'kylin home : ' ${KYLIN_HOME}

echo "Kylin server stop !"


if [ ${NEED_UPLOAD_KYBOT_FILE} = "true" ]; then
    echo "upload kybot files"
    KYBOT_PATH=$1
    #upload kybot zip package
    cd ${KYLIN_HOME}
    #copy kybot
    cp -r ${KYBOT_PATH} ./
    bash ./kybot/kybot.sh
    kybot_pkg_parent_name=`ls kybot_dump/ | grep kybot`
    kybot_pkg_name=`ls kybot_dump/kybot*/ | grep kybot`
    echo ${kybot_pkg_name}
    cd ../

    python ${PROJECT_BASE_DIR}/scripts/kybot-upload-client.py -s https://kybot.io -u ${KYBOT_ACCOUNT} -p ${KYBOT_PASSWORD} -f ${KYLIN_HOME}/kybot_dump/${kybot_pkg_parent_name}/${kybot_pkg_name}
fi