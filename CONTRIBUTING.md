# Contributing

Thanks for helping. The aim is to keep `avc` small, fast and honest about what it guarantees.

- **No dependencies.** Use the Go standard library only.
- **Run the unit tests** before sending a change: `go vet ./... && go test ./...`.
- **Benchmarks are heavy.** Run one at a time and start small, e.g. `--agents 10`. Report the machine, OS and Git version alongside any numbers.
- **Guarantees go in the README.** If a change affects durability, conflicts, leases or resource behaviour, update the "Contract and limits" section in the same pull request.
- **Bug reports:** give the exact commands, the exit codes you saw and the output of `avc stats`.
