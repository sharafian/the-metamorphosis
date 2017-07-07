#!/bin/bash

curl -X POST 'localhost:8080/rpc?prefix=example.&method=send_transfer' -d '[{"id":"8ef123cb-84cb-0724-153e-f23c1af5e997","executionCondition":"rdnt13O8gU7KJxacVIOJKc6dT2j5vqvutk3AWL6I-Js","to":"test.example.receiver","amount":"10","expiresAt":"2017-07-31T00:00:00Z","ilp":"ARgAAAAAAAAAAQ1leGFtcGxlLmFsaWNlAAA"}]' -H 'Content-Type: application/json'
