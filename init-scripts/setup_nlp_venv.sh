#!/bin/bash
set -e

VENV_PATH="/opt/airflow/venv_nlp"
# Dockerfile의 COPY 목적지인 /tmp/requirements_nlpvenv.txt와 일치시킴
REQ_PATH="/tmp/requirements_nlpvenv.txt"

echo "--- Creating NLP Virtual Environment ---"
python3 -m venv $VENV_PATH
$VENV_PATH/bin/pip install --upgrade pip
$VENV_PATH/bin/pip install --no-cache-dir -r $REQ_PATH
echo "--- Setup Complete ---"