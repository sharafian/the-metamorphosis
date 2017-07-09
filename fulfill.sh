#!/bin/bash

curl -X POST 'localhost:8080/rpc?prefix=example.&method=fulfill_condition' -d '["8ef123cb-84cb-0724-153e-f23c1af5e997","rdnt13O8gU7KJxacVIOJKc6dT2j5vqvutk3AWL6I-Js"]'
