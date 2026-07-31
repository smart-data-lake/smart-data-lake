#!/usr/bin/env bash
#
# Smart Data Lake - Build your data lake the smart way.
#
# Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

# Downloads a Spark distribution (if not already present) and starts a local Spark Connect server on port 15002.
# Used in CI for sdl-sparkconnect tests, but can also be used for local development.
# The Spark distribution and server working files are kept in the directory of this script.
# Exports SPARK_HOME to $GITHUB_ENV if running in GitHub Actions.
# Note that the sdl-sparkconnect tests can also start the server themselves if only SPARK_HOME is set,
# see SparkConnectTestUtil.
# The server is started with delta lake support, as SDLSaveMode.Merge and deleting partitions need
# a table format supporting row-level operations.

set -euo pipefail
cd "$(dirname "$0")"


if [ -z "${SPARK_VERSION}" ]; then
  export SPARK_VERSION=4.1.1
fi
SPARK_DIST=spark-${SPARK_VERSION}-bin-hadoop3
DELTA_VERSION=4.2.0

if [ ! -d "$SPARK_DIST" ]; then
  echo "downloading https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_DIST}.tgz"
  wget -nv --progress=dot:giga https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_DIST}.tgz
  tar -xzf ${SPARK_DIST}.tgz
  rm ${SPARK_DIST}.tgz
fi

export SPARK_HOME=$(pwd)/${SPARK_DIST}
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "SPARK_HOME=$SPARK_HOME" >> "$GITHUB_ENV"
fi

# Add delta lake jars to the server classpath. Note that using the --packages option instead does not work,
# as jars submitted with --packages are not visible to the classloader loading the session catalog plugin.
if [ ! -f "$SPARK_HOME/jars/delta-spark_4.1_2.13-${DELTA_VERSION}.jar" ]; then
  echo "downloading delta-spark libraries"
  wget -q -P "$SPARK_HOME/jars" https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/${DELTA_VERSION}/delta-spark_4.1_2.13-${DELTA_VERSION}.jar
  wget -q -P "$SPARK_HOME/jars" https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar
fi

# Remove state of previous server runs, so that the test server starts with a fresh catalog.
# Note that the default in-memory catalog forgets tables on restart, but their warehouse directories would persist
# and block creating tables with the same name again.
rm -rf spark-warehouse metastore_db derby.log

echo $"starting Spark Connect server"
"$SPARK_HOME/sbin/start-connect-server.sh" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# wait for the server to accept connections
for i in $(seq 1 60); do
  if (echo > /dev/tcp/localhost/15002) 2>/dev/null; then
    echo "Spark Connect server is listening on port 15002"
    exit 0
  fi
  sleep 1
done
echo "ERROR: Spark Connect server did not start within 60s" >&2
exit 1
