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
KYBOT_ACCOUNT=$2
KYBOT_PASSWD=$3

#set kylin home
cd ${BENCHMARK_WORKSPACE}/kap-*/
export KYLIN_HOME=`pwd`

# Tear down stage
${KYLIN_HOME}/bin/kylin.sh stop
echo 'kylin home : ' ${KYLIN_HOME}

echo "Kylin server stop !"

echo "upload kybot files"
#upload kybot zip package
cd ${KYLIN_HOME}
bash ./kybot/kybot.sh
kybot_pkg_parent_name=`ls kybot_dump/ | grep kybot`
kybot_pkg_name=`ls kybot_dump/kybot*/ | grep kybot`
echo ${kybot_pkg_name}
cd ../

python {BENCHMARK_WORKSPACE}/scripts/kybot-upload-client.py -s https://kybot.io -u ${KYBOT_ACCOUNT} -p ${KYBOT_PASSWD} -f ${KYLIN_HOME}/kybot_dump/${kybot_pkg_parent_name}/${kybot_pkg_name}