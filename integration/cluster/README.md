# Live HopsFS client checks

This command runs against a real deployment and emits JSON lines for each
check. It is separate from the local unit suite. It creates files only under
the supplied unique `/tmp/hopsfs-client-426-...` root and exits nonzero on a
failed check. It does not change server configuration or restart services.

Build with a supported Go toolchain and run from a host or pod with network
access to both NameNodes and DataNodes:

```sh
go build -race -tags netgo,osusergo -o cluster-check ./integration/cluster
./cluster-check -address namenode.hopsworks:8020 \
  -server-name namenode.hopsworks \
  -certs /srv/hops/super_crypto/hdfs \
  -root /tmp/hopsfs-client-426-UNIQUE-RUN -policy CLOUD
```

The certificate directory must contain `hops_ca_bundle.pem`,
`hdfs_certificate_bundle.pem` and `hdfs_priv.pem`, mounted read-only. Private
key material is never written to results. The command uses the HDFS identity;
it does not validate authorization for other users. TLS identity checking
remains enabled.

`-policy` labels the results. Configure the root's storage policy separately
if the deployment permits it. Cloud-only clusters cannot validate HOT/DB
storage or DB promotion. They also cannot validate transparent encryption
unless a KMS-backed encryption zone is configured. Multi-node outage and key
rotation tests require separate deployment setup.

The checks cover TLS identity rejection, concurrent defaults, unavailable
bootstrap-address fallback, multi-block
writes and append, repeated/empty append, small-file positions, tiny tail
reads, parallel positional reads through EOF, directory cursors, metadata
errors, expired deadlines, retained writer errors, repeated checksums,
idempotent close and connection release. Byte-for-byte reads verify written
payloads. A run with the race detector is a correctness check, not a production
throughput benchmark.

After collecting results, remove only this run's directory:

```sh
./cluster-check -address namenode.hopsworks:8020 \
  -root /tmp/hopsfs-client-426-UNIQUE-RUN -cleanup
```

Take and release the workspace cluster claim around the complete run and
cleanup sequence. Do not copy certificate or key contents into artifacts.
