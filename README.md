# elephant

[Absurd](https://earendil-works.github.io/absurd) is a Postgres-backend engine for [durable execution](https://earendil-works.github.io/absurd/concepts/) created by [Earendil Works](https://github.com/earendil-works). It has official SDKs for Go, Python and TypeScript, this crate aims to be the missing Rust SDK.

## Getting started

`elephant` uses [`sqlx`](https://docs.rs/sqlx/), thus it is recommended to integrate the installation of `absurd.sql` into your migration set:

```sh
(
  set -e
  schema=$(mktemp)
  trap 'rm -f "$schema"' 0
  curl -fL -o "$schema" \
    https://github.com/earendil-works/absurd/releases/download/0.5.0/absurd.sql
  printf '%s  %s\n' \
    d34309370c539f3a51f2b36b69b1f77551f8e4a14480a1c8def8bb8f40fd9aab "$schema" |
    sha256sum --check -
  mkdir -p migrations
  mv "$schema" "migrations/$(date -u +%Y%m%d%H%M%S)_absurd.sql"
)
```

The checksum is pinned to the `absurd.sql` release asset for Absurd `0.5.0`. A failed download or checksum mismatch leaves no migration file.
