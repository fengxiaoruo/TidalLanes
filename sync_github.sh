#!/bin/zsh
set -euo pipefail

cd "$(dirname "$0")"

message="${1:-Update $(date '+%Y-%m-%d %H:%M')}"

echo "Repo: $(pwd)"
git status --short

git add -A

if git diff --cached --quiet; then
  echo "No changes to commit."
  exit 0
fi

git commit -m "$message"
git push

echo "Sync complete."
