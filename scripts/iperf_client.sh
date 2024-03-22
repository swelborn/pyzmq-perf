#!/bin/bash

date
iperf -c CHANGEME -p 5558 -t 300 -e -P8 -i 30 -w 512M