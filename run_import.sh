#!/bin/bash

log_subscriber -p 6389 --channel ASN_History --log_path ./logs/ &

python asn_history.py

