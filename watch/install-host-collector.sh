#!/bin/sh
# Run once as root on an asaroton host the watch job should read (cluephone.org).
# Installs the read-only collector and allows the watch job's key to run that, and only that.
#
#   install-host-collector.sh /path/to/host_collect.py 'ssh-ed25519 AAAA… asaroton-watch'
set -eu
SRC=$1
KEY=$2
install -m 0755 -o root -g root "$SRC" /usr/local/sbin/asaroton-watch-collect
mkdir -p /root/.ssh && chmod 700 /root/.ssh
touch /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys
grep -v ' asaroton-watch$' /root/.ssh/authorized_keys > /root/.ssh/authorized_keys.new || true
echo "restrict,command=\"/usr/bin/python3 /usr/local/sbin/asaroton-watch-collect\" $KEY" >> /root/.ssh/authorized_keys.new
chmod 600 /root/.ssh/authorized_keys.new && mv /root/.ssh/authorized_keys.new /root/.ssh/authorized_keys
echo "installed; the key can only run /usr/local/sbin/asaroton-watch-collect"
