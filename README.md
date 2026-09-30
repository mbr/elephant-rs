# elephant

[Absurd](https://earendil-works.github.io/absurd) is a Postgres-backend engine for [durable execution](https://earendil-works.github.io/absurd/concepts/) created by [Earendil Works](https://github.com/earendil-works). It has official SDKs for Go, Python and TypeScript, this crate aims to be the missing Rust SDK.

## Getting started

`elephant` uses [`sqlx`](https://docs.rs/sqlx/), thus it is recommended to integrate the installation of `absurd.sql` into your migration set:

```sh
curl -fL --create-dirs -o "migrations/$(date -u +%Y%m%d%H%M%S)_absurd_0.5.0.sql" \
  https://github.com/earendil-works/absurd/releases/download/0.5.0/absurd.sql
```
