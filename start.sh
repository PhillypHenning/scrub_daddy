#! /bin/bash

if [[ -f "Alberta-output.txt" ]]; then
    rm Alberta-output.txt
fi
if [[ -f "Ontario-output.txt" ]]; then
    rm Ontario-output.txt
fi

source ./env/bin/activate
python scrub_daddy.py
