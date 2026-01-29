#!/bin/bash

if [ -z "$1" ]; then
    echo "Use: bash $0 \"migration description\""
    echo
    exit 1
fi

source .venv/bin/activate
alembic revision --autogenerate -m "$1"
 