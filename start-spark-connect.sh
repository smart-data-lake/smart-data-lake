wget https://archive.apache.org/dist/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz
tar -xzf spark-4.1.1-bin-hadoop3.tgz
echo "SPARK_HOME=$(pwd)/spark-4.1.1-bin-hadoop3" >> $GITHUB_ENV

$SPARK_HOME/sbin/start-connect-server.sh &
sleep 10  # Wait for server to start