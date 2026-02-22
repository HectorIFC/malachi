#!/bin/bash

# Setup Script - Install Lefthook and configure git hooks
# This script ensures all developers have the same git hooks

set -e

echo "🔧 Setting up development environment..."
echo ""

# Detect OS
OS="$(uname -s)"

# Install Lefthook if not present
if ! command -v lefthook &> /dev/null; then
  echo "📦 Installing Lefthook..."

  case "$OS" in
    Darwin*)
      # macOS
      if command -v brew &> /dev/null; then
        brew install lefthook
      else
        echo "❌ Homebrew not found. Please install Lefthook manually:"
        echo "   https://github.com/evilmartians/lefthook#install"
        exit 1
      fi
      ;;
    Linux*)
      # Linux - use go install or download binary
      if command -v go &> /dev/null; then
        go install github.com/evilmartians/lefthook@latest
      else
        echo "Installing Lefthook from binary..."
        curl -1sLf 'https://dl.cloudsmith.io/public/evilmartians/lefthook/setup.sh' | sudo -E bash
        sudo apt-get update
        sudo apt-get install -y lefthook
      fi
      ;;
    *)
      echo "❌ Unsupported OS: $OS"
      echo "Please install Lefthook manually:"
      echo "   https://github.com/evilmartians/lefthook#install"
      exit 1
      ;;
  esac

  echo "✅ Lefthook installed"
else
  echo "✅ Lefthook already installed"
fi

echo ""
echo "🔗 Installing git hooks..."

# Install lefthook hooks
lefthook install

echo ""
echo "✅ Setup complete!"
echo ""
echo "Git hooks are now configured. The pre-commit hook will:"
echo "  - Run benchmarks when lib/malachimq/ or benchmark/ files change"
echo "  - Update baseline_reference.json automatically"
echo "  - Stage the updated baseline"
echo ""
echo "To skip the hook temporarily:"
echo "  git commit --no-verify"
echo ""
