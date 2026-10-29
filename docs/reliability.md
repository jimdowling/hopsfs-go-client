# Connection and file contracts

NameNode connection setup and RPC requests default to 30-second timeouts.
`ClientOptions.NamenodeDialTimeout` controls TCP, TLS, and protocol setup.
`ClientOptions.NamenodeRequestTimeout` bounds RPC queueing, connection setup,
failover, and the response. A negative value disables the corresponding default.
An explicit operation context or file deadline can impose an earlier limit.
`Client.Close` interrupts active NameNode I/O and stops lease renewal. Repeated
calls are safe.

`FileReader.SetDeadline` and `FileWriter.SetDeadline` apply to subsequent
DataNode connection setup, authentication, and I/O. Writer deadlines also
bound block-allocation RPCs, completion RPCs, and their retry delays. A zero
file deadline leaves those operations subject to configured client defaults.

NameNode response bodies default to a 128 MiB limit, configurable with
`NamenodeMaxResponseLength`. DataNode control messages and SASL records are
limited to 1 MiB. Block packets are limited to 16 MiB, following Hadoop's
`PacketReceiver.MAX_PACKET_SIZE`. Malformed lengths, missing required
protobuf fields, and inconsistent checksum layouts return errors.

# TLS and KMS

TLS connections verify the CA chain and the DNS or IP identity in the
connection address, using TLS 1.2 or later. For a proxy or address alias,
set `ClientOptions.TLSServerName` to the identity present in the NameNode
certificate. This override changes the identity checked and does not disable
certificate verification. A custom `NamenodeDialFunc` also applies to TLS
connections.

KMS HTTP requests default to a 30-second timeout and a 1 MiB response-body
limit. Configure these with `KMSTimeout` and `KMSMaxResponseLength`. A negative
`KMSTimeout` disables that timeout. Data-transfer keys are refreshed when
expired and once after an explicit unknown-key response. Concurrent cache
loads share one request.

# File operations

DataNode chunks with supplied checksums pass validation before being returned.
On a replica failure, the next replica starts at the first byte not yet
verified. `ReadAt` uses an independent
cursor and connection, supports parallel calls, and preserves the sequential
reader position. Directory reads retain unused server-page entries and
continue from the current position when called with a non-positive count.

Call `FileWriter.Close` to flush data and complete the file. Terminal I/O
failure stops pipeline workers, closes the DataNode connection, releases the
lease, and remains visible on subsequent calls. If completion is still
pending, `Close` can return `ErrReplicating` wrapped in `os.PathError`; retry
`Close` to finish. `FileWriter.Abort` discards buffered data and stops lease
renewal without completing or deleting the file. Data already acknowledged
by DataNodes remains subject to NameNode lease recovery.

`Walk` retains a fresh status request for each visited child and does not
fetch encryption keys for files it only visits.

# Local verification

Run `make unit-test` for the local protocol, lifecycle, configuration,
cryptography, and API-contract tests with the race detector and `go vet`.
These tests use in-memory peers and loopback servers. `make test` requires
the filesystem fixtures described by the integration setup.

Useful parser fuzz targets are `FuzzReadRPCPacket`, `FuzzReadPrefixedMessage`,
`FuzzBlockReadStream`, and `FuzzProtectedStreamDecode`. Run each target in
its package using `go test -run '^$' -fuzz <target> -fuzztime 30s`.
