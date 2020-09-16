#!/bin/bash
set -x
set -e

#######################################################################################################################
# Initial set up                                                                                                      #
#######################################################################################################################

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
BASE_DIR="$SCRIPT_DIR/.."

buildUrl="http://artifactory.uber.internal:4587/artifactory/pub/presto/builds/"

dependenciesUrl="http://artifactory.uber.internal:4587/artifactory/pub/presto/misc/"

#######################################################################################################################
# Fetch Commit Sha                                                                                                    #
#######################################################################################################################

commit_sha=${GIT_REF:0:7}
echo ${commit_sha}

#######################################################################################################################
# Fetch versioned presto binary from Artifactory                                                                      #
#######################################################################################################################

	ARTIFACTORY_BUILD_OP=$(curl "http://artifactory.uber.internal:4587/artifactory/api/storage/pub/presto/builds/")
echo ${ARTIFACTORY_BUILD_OP}

ARTIFACT_NAME=$(curl "http://artifactory.uber.internal:4587/artifactory/api/storage/pub/presto/builds/" 2> /dev/null | grep uri | grep ${commit_sha} | awk '{ print $3 }' | sort | head -1 | sed s/\"//g | sed s/,//g )
expectedMasterBinaryPattern="presto-server_master_${commit_sha}"
expectedOnDemandBinaryPattern="presto-server_ondemand_${commit_sha}"

trim_art_name=`echo ${ARTIFACT_NAME}|sed 's/\///g'`

if [[ ${trim_art_name} == *${expectedMasterBinaryPattern}* || ${trim_art_name} == *${expectedOnDemandBinaryPattern}* ]]; then
  echo "Artifact present in Artifactory"
else
  echo "Artifact is not present in Artifactory"
  exit 1
fi

wget --no-verbose "${buildUrl}${ARTIFACT_NAME}" -O presto-server.tar.gz
mkdir presto-docker
tar xf presto-server.tar.gz -C presto-docker --strip-components 1
rm presto-server.tar.gz

#######################################################################################################################
# Setup dependencies from artifactory                                                                                 #
#######################################################################################################################

JVMKILL_URL="${dependenciesUrl}libjvmkill.so"
JVMKILL_LIB_NAME=$(echo "$JVMKILL_URL" | rev | cut -d'/' -f 1 | rev | cut -d'?' -f 1)
wget --no-verbose "$JVMKILL_URL" -O "$JVMKILL_LIB_NAME"
mv "$JVMKILL_LIB_NAME" presto-docker/bin/

#######################################################################################################################
# Done.                                                                                                               #
#######################################################################################################################
