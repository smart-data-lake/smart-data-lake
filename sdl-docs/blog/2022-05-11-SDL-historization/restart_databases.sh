
# shutdown, cleanup
podman stop mssql
rm -r data/_metastore data/int-* data/spark-warehouse data/state
mkdir -p data/_metastore

# startup
set -e # exit on errors
./part2/podman-compose.sh #use the script from the getting-started guide
podman run -d --pod sdl_sql --hostname mssqlserver --add-host mssqlserver:127.0.0.1 --name mssql -v ${PWD}/data:/data  -v ${PWD}/config:/config -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=%abcd1234%" mcr.microsoft.com/mssql/server:2017-latest
sleep 10  ### wait until the MSSQL server is ready (maybe a very conservative time)
podman exec -it mssql /opt/mssql/bin/mssql-conf set sqlagent.enabled true
podman stop mssql
podman start mssql
podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -i /config/db_init_chess.sql
