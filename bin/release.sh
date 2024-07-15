set -e

git cz bump --files-only

VERSION=$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "exon") | .version')

# if VERSION is empty, then exit
if [ -z "$VERSION" ]; then
  echo "Version is empty"
  exit 1
fi

echo "Releasing v$VERSION"

sleep 1

git add -u .
git commit -m "release: bump to version v$VERSION"
git tag -a v$VERSION -m "release: v$VERSION"

git push origin main
git push origin v$VERSION
