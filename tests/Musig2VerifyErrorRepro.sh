#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
export script_dir

curl -v --location 'http://127.0.0.1:21714' --json '{
  "id": "42",
  "jsonrpc": "2.0",
  "method": "connect_peer",
  "params": [
    {"address": "/ip4/127.0.0.1/tcp/8345/p2p/QmSRcPqUn4aQrKHXyCDjGn2qBVf43tWBDS2Wj9QDUZXtZp"}
  ]
}'

curl -v --location 'http://127.0.0.1:21714' --json '{
  "id": "42",
  "jsonrpc": "2.0",
  "method": "connect_peer",
  "params": [
    {"address": "/ip4/127.0.0.1/tcp/8346/p2p/QmaFDJb9CkMrXy7nhTWBY5y9mvuykre3EzzRsCJUAVXprZ"}
  ]
}'

bash -c '(for i in {1..50000}; do
  "$script_dir/deploy/generate-blocks.sh" 10
  sleep 1
done)' &

cleanup() {
  echo "=> cleanup"
  pkill -f generate-blocks.sh
}

trap cleanup EXIT INT TERM

# 循环50次
for i in {1..50}; do
  # 第一个curl请求
  echo "Opening channels, iteration $i"

  curl --location 'http://127.0.0.1:21714' --json '{
    "id": 42,
    "jsonrpc": "2.0",
    "method": "open_channel",
    "params": [
        {
            "peer_id": "QmSRcPqUn4aQrKHXyCDjGn2qBVf43tWBDS2Wj9QDUZXtZp",
            "funding_amount": "0x3c5986200",
            "public": true
        }
    ]
  }'

  # 等待1秒
  sleep 1

  # 第二个curl请求
  curl --location 'http://127.0.0.1:21714' --json '{
    "id": 42,
    "jsonrpc": "2.0",
    "method": "open_channel",
    "params": [
        {
            "peer_id": "QmaFDJb9CkMrXy7nhTWBY5y9mvuykre3EzzRsCJUAVXprZ",
            "funding_amount": "0x3c5986200",
            "public": true
        }
    ]
  }'

  # 再次等待1秒
  sleep 1
done
