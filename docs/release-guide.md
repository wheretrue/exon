# How to Release `Exon`

First bump with `git cz bump --files-only`, so the files are changed, but no commit is made. This is let's Cargo pickup the new version and update its lock file.

```bash
git cz bump --files-only --increment PATCH
```

Next, get the new version from `Cargo.toml` workspace and tag it.

```bash
VERSION=$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "exon") | .version')
echo "Releasing v$VERSION"
```

Then commit the changes and tag it.

```bash
git add -u .
git commit -m "release: bump to version v$VERSION"
git tag -a v$VERSION -m "release: v$VERSION"
```

Finally, push the changes and tag.

```bash
git push origin main
git push origin v$VERSION
```
