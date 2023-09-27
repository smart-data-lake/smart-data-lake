#!/usr/bin/env bash

# PYTHON INDEX BUILDER
set -e
python3 -m venv .venv
source "./.venv/bin/activate"
pip3 install -r ./requirements.txt
python3 ./build_index.py $1 $2
deactivate