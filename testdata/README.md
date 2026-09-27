# Test fixtures

`absurd.sql` is the release asset from `earendil-works/absurd` version `0.5.0`,
commit `550d3b9e6f9382d96178de6ab8c90c7f8edf2227`.

Update it deliberately when testing compatibility with a newer Absurd schema.

`go-checkpoints/checkpoints.json` contains actual PostgreSQL checkpoints written
by the Go SDK at the same pinned revision. `go-checkpoints/main.go` generates
the fixture and can replay it without executing step bodies or emitting events.
The Rust integration test replays it and independently writes matching state.
It also exercises child-result decoding of Go-serialized terminal snapshots.

To regenerate and verify from the repository root (Go 1.25+ and `pgdb` required):

```sh
go -C testdata/go-checkpoints build -o /tmp/elephant-go-checkpoints .
pgdb -t -F sh -c '/tmp/elephant-go-checkpoints > /tmp/go-checkpoints.json'
diff -u testdata/go-checkpoints/checkpoints.json /tmp/go-checkpoints.json
pgdb -t -F /tmp/elephant-go-checkpoints testdata/go-checkpoints/checkpoints.json
```

Go is only needed when verifying or updating the fixture, not for normal Rust
checks. The generator's module and checksums pin its SDK and driver dependencies.
