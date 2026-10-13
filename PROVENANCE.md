# Provenance

Everything in this repository was written for it, on personal time, by Tran Trung Dung.

- An event pipeline and the harness that measures its write path. No employer code, no client code, no proprietary data. Nothing here is
  extracted from a product I worked on; it talks to a ClickHouse and a Vector it starts itself through `docker compose`; the events are generated in this repository.
- Third-party code is not vendored: the dependencies are none: `go.mod` has no require lines at all, and each keeps its own licence.
- The diagrams are drawn here. The commit history is the history of this repository.
- The measured numbers quoted in the README come from the harness in this repository, and `make measure` is the command that produced them.

If you believe something here belongs to you, open an issue and it will be dealt with.
