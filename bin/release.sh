git cz bump --files-only

VERSION=$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "exon") | .version')
echo "Releasing v$VERSION"

sleep 1

git add -u .
git commit -m "release: bump to version v$VERSION"
git tag -a v$VERSION -m "release: v$VERSION"

git push origin main
git push origin v$VERSION
