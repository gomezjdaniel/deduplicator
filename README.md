# deduplicator

Opens a TCP listener in a given port, reads-in from clients a list of numbers and
writes into an output file a deduplicated number list.

## Build

`make build` generates an executable in `bin/deduplicator`.

## Run

`make run` runs the executable with sane configuration values.
Execute `bin/deduplicator --help` to see about them.

## Test

`make test` runs tests.
