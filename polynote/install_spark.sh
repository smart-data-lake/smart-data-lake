#!/bin/bash
# This script is intended for use from the docker builds.

set -e -x

SPARK_VERSION=${SPARK_VERSION:-"3.2.1"}
SPARK_VERSION_DIR="spark-${SPARK_VERSION}"

if test "${SPARK_VERSION}" \> "3" -a "${SCALA_VERSION}" = "2.11"; then
  echo "Spark ${SPARK_VERSION} is not available for Scala ${SCALA_VERSION}. Setting Scala version to 2.12."
  SCALA_VERSION="2.12"
fi

if test "${SPARK_VERSION}" \< "3" -a "${SCALA_VERSION}" = "2.12"; then
  echo "Spark ${SPARK_VERSION} is not available for Scala ${SCALA_VERSION}. Setting Scala version to 2.11."
  SCALA_VERSION="2.11"
fi

SPARK_NAME="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"

pushd /opt
wget --progress=dot:giga "https://dlcdn.apache.org/spark/${SPARK_VERSION_DIR}/${SPARK_NAME}.tgz"
tar zxpf "${SPARK_NAME}.tgz"
mv "${SPARK_NAME}" spark
rm "${SPARK_NAME}.tgz"
popd

if test -z "${SPARK_DIST_CLASSPATH}"; then
  echo "Skipping spark env"
else
  echo "export SPARK_DIST_CLASSPATH=\"${SPARK_DIST_CLASSPATH}\"" > /opt/spark/conf/spark-env.sh
fi
