#!/data/data/com.termux/files/usr/bin/bash

set -e

echo "Updating packages..."
pkg update -y

echo "Upgrading packages..."
pkg upgrade -y

echo "Installing packages..."
pkg install openssh tmux git curl wget nano vim python -y

echo "Checking installations..."

ssh -V
tmux -V
python --version

echo "Setup completed successfully!"