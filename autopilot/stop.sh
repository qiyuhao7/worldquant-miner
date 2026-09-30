#!/bin/bash
# Stop the mining daemon. Usage: bash autopilot/stop.sh
pkill -f "autopilot/miner_loop.py" && echo "daemon stopping" || echo "no daemon found"
