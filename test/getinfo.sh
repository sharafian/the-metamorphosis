#!/bin/bash

http -j POST localhost:8080/rpc prefix==test.example. method==get_info
