#!/bin/bash
# Deployment script — run on the droplet to pull latest and restart
set -e

APP_DIR="/opt/incentive-calculator"

cd "$APP_DIR"
git pull origin main
npm install --production
pm2 restart incentive-calculator || pm2 start ecosystem.config.js
echo "Deployed successfully at $(date)"
