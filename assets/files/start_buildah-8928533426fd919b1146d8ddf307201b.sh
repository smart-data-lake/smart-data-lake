#!/bin/bash

for arg do
  shift
  [ "$arg" = "--rm" ] && continue
  [ "$arg" = "run" ] && continue
  [ "$arg" = "-v" ] && store="$store $arg" && take_next=1 && continue
  [ "$take_next" = "1" ] && take_next=0 && store="$store $arg" && continue
  [ "$arg" = "airbyte-mssql" ] && continue
  set -- "$@" "$arg"
done
buildah --storage-driver=vfs $store run --isolation=chroot airbyte-mssql /bin/bash base.sh $@
