#!/bin/bash
#set -o errexit

trap 'java -jar /opt/derby/lib/derbyrun.jar server shutdown' SIGTERM

java_vm_parameters="-Dfile.encoding=UTF-8"
if [ -n "${JAVA_VM_PARAMETERS}" ]; then
  java_vm_parameters=${JAVA_VM_PARAMETERS}
fi

db_user="sa"
if [ -n "${DB_USER}" ]; then
  db_user=${DB_USER}
fi

db_password="1234"
if [ -n "${DB_PASSWORD}" ]; then
  db_password=${DB_PASSWORD}
fi

mkdir -p $DB_DIR

java -Dderby.system.home=$DB_DIR -Dderby.drda.host=0.0.0.0 -Dderby.authentication.provider=BUILTIN -Dderby.user.$db_user=$db_password $java_vm_parameters -jar /opt/derby/lib/derbyrun.jar server start
