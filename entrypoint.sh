#!/bin/bash

jupyter_lab_path=$(which jupyter)

if [ -z "$jupyter_lab_path" ]; then
  echo "Jupyter not found"
  exit 1
fi

rm -rf \
  /home/pysus/.local/share/jupyter/runtime \
  /home/pysus/.local/share/jupyter/notebook_secret \
  /home/pysus/.jupyter/lab/workspaces

mkdir -p /home/pysus/.jupyter

cat > /home/pysus/.jupyter/jupyter_server_config.py << 'EOF'
import logging

logging.getLogger("jupyter_server.services.kernels").setLevel(logging.ERROR)
EOF

$jupyter_lab_path lab --ip=0.0.0.0 --ServerApp.open_browser=False --ServerApp.default_url=/lab/tree/Welcome.ipynb --NotebookApp.token='' --NotebookApp.password='' &

sleep 3

echo "Open http://127.0.0.1:8888/lab in your browser"

wait
