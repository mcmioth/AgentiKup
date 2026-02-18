#!/bin/bash
# Avvia la dashboard OpenCUP
# Uso: bash run.sh

set -e
cd "$(dirname "$0")"

# Controlla che il Parquet esista
if [ ! -f "data/progetti.parquet" ]; then
    echo "File Parquet non trovato. Eseguo la conversione..."
    python scripts/convert_to_parquet.py
fi

echo ""
echo "==============================="
echo " OpenCUP Dashboard"
echo " http://localhost:8000"
echo "==============================="
echo ""

uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
