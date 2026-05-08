#!/usr/bin/env bash
#
# Cut a release: bump pyproject.toml, commit, tag, push, create a GitHub Release.
# Prints the command for triggering publish.yml manually after the release is up.
#
# Usage: scripts/release.sh v<MAJOR.MINOR.PATCH> [--dry-run]

set -euo pipefail

INPUT="${1:?usage: scripts/release.sh v<MAJOR.MINOR.PATCH> [--dry-run]}"
DRY_RUN=false
[[ "${2:-}" == "--dry-run" ]] && DRY_RUN=true

# Strict vX.Y.Z only — no pre-release suffixes.
[[ "$INPUT" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] \
  || { echo "error: version must be vX.Y.Z (got '$INPUT')"; exit 1; }
TAG="$INPUT"
VERSION="${INPUT#v}"  # strip 'v' for pyproject.toml (PEP 440 disallows it).

# Pre-flight: clean tree, on main, in sync, tag doesn't exist, version changed.
[[ -z "$(git status --porcelain)" ]] || { echo "error: working tree dirty"; exit 1; }
[[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || { echo "error: not on main"; exit 1; }
git fetch origin main --tags --quiet
[[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/main)" ]] \
  || { echo "error: local main not in sync with origin/main"; exit 1; }
if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "error: tag $TAG already exists"; exit 1
fi

CURRENT="$(grep -E '^version = ' pyproject.toml | head -1 | sed -E 's/version = "(.*)"/\1/')"
[[ "$CURRENT" != "$VERSION" ]] || { echo "error: pyproject.toml already at $VERSION"; exit 1; }

echo "Running make check..."
make check

# Preview release notes before committing anything irreversible.
PREV_TAG="$(git tag --list 'v*' --sort=version:refname | tail -1)"
NOTES_ARGS=(-f tag_name="$TAG" -f target_commitish="$(git rev-parse HEAD)")
[[ -n "$PREV_TAG" ]] && NOTES_ARGS+=(-f previous_tag_name="$PREV_TAG")

echo
echo "=== Auto-generated release notes preview ==="
gh api repos/{owner}/{repo}/releases/generate-notes "${NOTES_ARGS[@]}" --jq .body
echo "============================================"
echo

if $DRY_RUN; then
  echo "Dry run: stopping before any changes."
  exit 0
fi

read -rp "Proceed with release $TAG? [y/N] " ans
[[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "aborted"; exit 1; }

# Bump, commit, tag, push.
sed -i.bak -E "s/^version = \".*\"/version = \"$VERSION\"/" pyproject.toml
rm pyproject.toml.bak
# Refresh the lockfile so the embedded project version matches pyproject.toml.
# --no-upgrade keeps transitive dependencies pinned for a deterministic release.
uv lock --no-upgrade
git add pyproject.toml uv.lock
git commit -m "Release $TAG"
git tag -a "$TAG" -m "Release $TAG"
git push origin main "$TAG"

# Create the GitHub Release with auto-generated notes (the same ones previewed above).
gh release create "$TAG" --title "$TAG" --generate-notes

echo
echo "Release $TAG created. Smoke-test on testpypi first, then publish to pypi:"
echo "  gh workflow run publish.yml -f release_tag=$TAG -f index=testpypi"
echo "  gh workflow run publish.yml -f release_tag=$TAG -f index=pypi"
echo "Or run the workflow from the GitHub Actions UI."
