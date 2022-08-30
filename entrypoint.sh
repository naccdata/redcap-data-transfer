#!/bin/sh

env >> /etc/environment

# execute CMD
echo "PYTHONPATH:" $PYTHONPATH
echo "$@"
exec "$@"
