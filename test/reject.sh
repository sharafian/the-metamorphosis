#!/bin/bash

curl -X POST 'localhost:8080/rpc?prefix=test.south.&method=reject_incoming_transfer' -d '["8ef123cb-84cb-0724-153e-f23c1af5e997",{"reason":"something"}]' -H 'Content-Type: application/json' -H 'Authorization: Bearer secret'
