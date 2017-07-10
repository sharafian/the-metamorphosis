#!/bin/bash

curl -X POST 'localhost:8080/rpc?prefix=test.east.&method=send_transfer' -d '[{"id":"8ef123cb-84cb-0724-153e-f23c1af5e997","executionCondition":"rdnt13O8gU7KJxacVIOJKc6dT2j5vqvutk3AWL6I-Js","to":"test.east.client","amount":"10","expiresAt":"2017-07-31T00:00:00Z","ilp":"ARYAAAAAAAAAAQt0ZXN0LnNvdXRoLgAA"}]' -H 'Content-Type: application/json' -H 'Authorization: Bearer placeholder'
