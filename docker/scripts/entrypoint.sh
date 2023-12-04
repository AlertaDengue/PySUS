#!/bin/bash

jupyter_lab_path=$(which jupyter)

if [ -z "$jupyter_lab_path" ]; then
  echo "Jupyter not found"
  exit 1
fi

$jupyter_lab_path lab --browser='firefox' --allow-root --NotebookApp.token='' --NotebookApp.password=''
