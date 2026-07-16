#!/bin/bash
#
# Smart Data Lake Builder - Build your data lake the smart way.
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

op=$1

# assert files given as parameters exist
JSONFILE_PARAM_CNT=0
for p in "$@"; do
    if [[ "$p" == *.json ]]; then
        if [ ! -f "$p" ]; then
            echo "file $p does not exist"
            exit -1
        fi
        ((JSONFILE_PARAM_CNT = JSONFILE_PARAM_CNT + 1))
    fi
done

case $op in

spec)
    if [[ $JSONFILE_PARAM_CNT != 0 ]]; then
        echo '{"type": "LOG", "log": {"level": "ERROR", "message": "0 jsonfile parameter expected, got '$JSONFILE_PARAM_CNT'"}}'
        exit -1
    fi
    echo '{"type": "SPEC", "spec": {"documentationUrl": "https://docsurl.com", "connectionSpecification": {"$schema": "http://json-schema.org/draft-07/schema#", "title": "Parser Spec", "type": "object", "required": ["my-config"], "additionalProperties": false, "properties": {"my-config": {"type": "string", "description": "test config"}}}}}'
    ;;

check)
    if [[ $JSONFILE_PARAM_CNT != 1 ]]; then
        echo '{"type": "LOG", "log": {"level": "ERROR", "message": "1 jsonfile parameter expected, got '$JSONFILE_PARAM_CNT'"}}'
        exit -1
    fi
    echo '{"type": "CONNECTION_STATUS", "connectionStatus": {"status": "SUCCEEDED"}}'
    ;;

discover)
    if [[ $JSONFILE_PARAM_CNT != 1 ]]; then
        echo '{"type": "LOG", "log": {"level": "ERROR", "message": "1 jsonfile parameter expected, got '$JSONFILE_PARAM_CNT'"}}'
        exit -1
    fi
    echo '{ "type": "CATALOG", "catalog": { "streams": [ { "name": "mystream", "json_schema": { "$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": { "produkttyp": { "type": "string" }, "flag": { "type": "boolean" }, "artikelID": { "type": "string" }, "price": { "type": "number" }, "artikelbezeichnung": { "type": "string" }, "updated": { "type": "string", "format": "date-time" }, "updatedNTZ": { "type": "string", "format": "date-time", "airbyte_type": "timestamp_without_timezone" } } }, "supported_sync_modes": [ "full_refresh" ] } ] } }'
    ;;

read)
    if [[ $JSONFILE_PARAM_CNT != 2 ]]; then
        echo '{"type": "LOG", "log": {"level": "ERROR", "message": "2 jsonfile parameter expected, got '$JSONFILE_PARAM_CNT'"}}'
        exit -1
    fi
    echo '{"type": "RECORD", "record": {"stream": "mystream", "data": {"produkttyp": "TEST", "flag": true, "artikelID": "123", "price": 2345.67, "artikelbezeichnung": "Test Auto", "updated": "2022-11-22T01:23:45", "updatedNTZ": "2022-11-22T01:23:45"}, "emitted_at": 1640029476000}}'
    ;;

esac
