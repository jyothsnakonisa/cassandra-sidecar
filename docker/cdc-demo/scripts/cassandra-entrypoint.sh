#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Patches CDC settings into the stock cassandra.yaml and enables remote JMX,
# then hands off to the original Docker entrypoint.
set -e

YAML="/etc/cassandra/cassandra.yaml"

patch_yaml() {
    local key="$1" value="$2"
    if grep -q "^${key}:" "$YAML"; then
        sed -i "s|^${key}:.*|${key}: ${value}|" "$YAML"
    elif grep -q "^#.*${key}:" "$YAML"; then
        sed -i "s|^#.*${key}:.*|${key}: ${value}|" "$YAML"
    else
        echo "${key}: ${value}" >> "$YAML"
    fi
}

# commitlog and cdc_raw must share the same filesystem for CDC hard-links.
patch_yaml "commitlog_directory"   "/var/lib/cassandra/commitlog"
patch_yaml "cdc_enabled"           "true"
patch_yaml "cdc_raw_directory"     "/var/lib/cassandra/cdc_raw"
patch_yaml "cdc_on_repair_enabled" "false"

# Cassandra 4.x uses cdc_total_space_in_mb; 5.x uses cdc_total_space.
if grep -q "cdc_total_space_in_mb" "$YAML"; then
    patch_yaml "cdc_total_space_in_mb" "4096"
else
    patch_yaml "cdc_total_space" "4096MiB"
fi

# Enable remote JMX so the sidecar can connect from its shared network namespace.
for opts_file in /etc/cassandra/jvm-server.options \
                 /etc/cassandra/jvm11-server.options \
                 /etc/cassandra/jvm17-server.options; do
    [ -f "$opts_file" ] && {
        echo "-Dcom.sun.management.jmxremote.local.only=false" >> "$opts_file"
        echo "-Djava.rmi.server.hostname=cassandra"            >> "$opts_file"
    }
done

exec /usr/local/bin/docker-entrypoint.sh "$@"
