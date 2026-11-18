#!/bin/zsh
# Commit the project snapshot after showing what will be staged.
# Usage: ./sync_github.sh "commit message" [--push]
# Nothing is committed without typing "y"; nothing is pushed without --push.
set -euo pipefail

cd "$(dirname "$0")"

message="${1:-Update $(date '+%Y-%m-%d %H:%M')}"
push="${2:-}"

echo "Repo:   $(pwd)"
echo "Branch: $(git branch --show-current)"
echo

git add -A --dry-run | sed 's/^/  /'
count=$(git add -A --dry-run | wc -l | tr -d ' ')

if [ "$count" = "0" ]; then
  echo "No changes to commit."
  exit 0
fi

echo
echo "$count paths would be staged. Check the list above for data, media, or secrets."
printf "Commit them with message \"%s\"? [y/N] " "$message"
read -r answer
if [ "$answer" != "y" ]; then
  echo "Aborted. Nothing staged."
  exit 1
fi

git add -A
git commit -m "$message"

if [ "$push" = "--push" ]; then
  git push
  echo "Committed and pushed."
else
  echo "Committed locally. Run again with --push, or 'git push', to upload."
fi
