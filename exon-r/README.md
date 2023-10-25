# R Bindings for Exon

## Installation

The easiest way to install the package is via [r-universe][]:

```r
install.packages('exonr', repos = c('https://wheretrue.r-universe.dev', 'https://cloud.r-project.org'))
```

## Documentation

The documentation is available at <https://wheretrue.r-universe.dev/exonr/doc/manual.html>.

## Development

To release:

1. Bump the version in `DESCRIPTION` file manually and commit.
2. Run `git tag exonr-<version>`. Where the version is the same as in the `DESCRIPTION` file.
3. Push the tag to GitHub.

Based on [extendr][].

[extendr]: https://extendr.github.io/rextendr/articles/package.html
[r-universe]: https://wheretrue.r-universe.dev/exonr
