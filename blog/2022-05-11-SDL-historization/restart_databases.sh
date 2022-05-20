podman stop mssql
podman-compose down ### This also deletes the mssql container
rm -r data/_metastore data/int-* data/spark-warehouse data/state
podman-compose up -d
podman run -d --pod sdl_sql --hostname mssqlserver --add-host mssqlserver:127.0.0.1 --name mssql -v ${PWD}/data:/data  -v ${PWD}/config:/config -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=%abcd1234%" mcr.microsoft.com/mssql/server:2017-latest
sleep 10  ### wait until the MSSQL server is ready (maybe a very conservative time)
podman exec -it mssql /opt/mssql/bin/mssql-conf set sqlagent.enabled true
podman stop mssql
podman start mssql
podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -i /config/db_init_chess.sql
