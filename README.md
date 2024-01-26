# gpbackup for CloudberryDB

[![Slack](https://img.shields.io/badge/Join_Slack-6a32c9)](https://communityinviter.com/apps/cloudberrydb/welcome)
[![Twitter Follow](https://img.shields.io/twitter/follow/cloudberrydb)](https://twitter.com/cloudberrydb)
[![Website](https://img.shields.io/badge/Visit%20Website-eebc46)](https://cloudberrydb.org)
[![GitHub Discussions](https://img.shields.io/github/discussions/cloudberrydb/cloudberrydb)](https://github.com/orgs/cloudberrydb/discussions)
![GitHub License](https://img.shields.io/github/license/cloudberrydb/gpbackup)

---

`gpbackup` and `gprestore` are Go utilities for performing Greenplum database
backups, which are originally developed by the Greenplum Database team. This
repo is a fork of gpbackup, dedicated to supporting CloduberryDB 1.0+. You
will feel no change using gpbackup in CloudberryDB as well as in Greenplum.

## Pre-Requisites

The project requires the Go Programming language version 1.11 or higher.
Follow the directions [here](https://golang.org/doc/) for installation, usage
and configuration instructions. Make sure to set the [Go PATH environment
variable](https://go.dev/doc/install) before starting the following steps.

## Downloading & Building

1. Downloading the latest version of this repo:

    ```bash
    go install github.com/cloudberrydb/gpbackup@latest
    ```

    This will place the code in `$GOPATH/pkg/mod/github.com/cloudberrydb/gpbackup`.

2. Entering the directory of `cloudberrydb/gpbackup`. Then, building and installing binaries of source code:

    ```bash
    cd <$GOPATH/pkg/mod/github.com/cloudberrydb/gpbackup>
    make depend
    make build
    ```

    You might encounter the `fatal: Not a git repository (or any of the parent directories): .git` prompt after running `make depend`. Ignore this prompt, because this does not affect the building.

    The `build` target will put the `gpbackup` and `gprestore` binaries in
    `$HOME/go/bin`. This will also attempt to copy `gpbackup_helper` to the
    CloudberryDB segments (retrieving hostnames from `gp_segment_configuration`).
    Pay attention to the output as it will indicate whether this operation was
    successful.

    `make build_linux` is for cross-compiling on macOS, and the target is Linux.

    `make install` will scp the `gpbackup_helper` binary (used with -single-data-file flag) to all hosts.

3. Checking whether the build is successful by checking whether your `$HOME/go/bin` directory contains `gpback`, `gprestore`, and `gpbackup_helper`.

    ```bash
    ls $HOME/go/bin
    ```

4. Validating whether the installation is successful:

    ```bash
    gpbackup --version
    gprestore --version
    ```

## Running the utilities

The basic command for gpbackup is
```bash
gpbackup --dbname <your_db_name>
```

The basic command for gprestore is
```bash
gprestore --timestamp <YYYYMMDDHHMMSS>
```

Run `--help` with either command for a complete list of options.

## Validation and code quality

### Test setup

Required for Cloudberry Database 1.0+, several tests require the
`dummy_seclabel` Cloudberry Database contrib module. This module exists only
to support regression testing of the SECURITY LABEL statement. It is not
intended to be used in production. Use the following commands to install the
module.

```bash
pushd $(find ~/workspace/cbdb -name dummy_seclabel)
    make install
    gpconfig -c shared_preload_libraries -v dummy_seclabel
    gpstop -ra
    gpconfig -s shared_preload_libraries | grep dummy_seclabel
popd
```

### Test execution

**NOTE**: The integration and end_to_end tests require a running Cloudberry
Database instance.

* To run all tests except end-to-end (linters, unit, and integration), use `make test`.
* To run only unit tests, use `make unit`.
* To run only integration tests (requires a running CloudberryDB instance), use `make integration`.
* To run end to end tests (requires a running CloudberryDB instance), use `make end_to_end`.

We provide the following targets to help developers ensure their code fits
Go standard formatting guidelines:

* To run a linting tool that checks for basic coding errors, use: `make lint`.
This target runs [gometalinter](https://github.com/alecthomas/gometalinter).
Note: The lint target will fail if code is not formatted properly.

* To automatically format your code and add/remove imports, use `make format`.
This target runs
[goimports](https://godoc.org/golang.org/x/tools/cmd/goimports) and
[gofmt](https://golang.org/cmd/gofmt/). We will only accept code that has been
formatted using this target or an equivalent `gofmt` call.

### Cleaning up

To remove the compiled binaries and other generated files, run `make clean`.

## Code Formatting

We use `goimports` to format go code. See
https://godoc.org/golang.org/x/tools/cmd/goimports The following command
formats the gpbackup codebase excluding the vendor directory and also lists
the files updated.

```bash
goimports -w -l $(find . -type f -name '*.go' -not -path "./vendor/*")
```

## Troubleshooting

1. Dummy Security Label module is not installed or configured

If you see errors in many integration tests (below), review the Validation and
code quality [Test setup](##Test setup) section above:

```
SECURITY LABEL FOR dummy ON TYPE public.testtype IS 'unclassified';
      Expected
          <pgx.PgError>: {
              Severity: "ERROR",
              Code: "22023",
              Message: "security label provider \"dummy\" is not loaded",
```

2. Tablespace already exists

If you see errors indicating the `test_tablespace` tablespace already exists
(below), execute `psql postgres -c 'DROP TABLESPACE test_tablespace'` to
cleanup the environment and rerun the tests.

```
    CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'
    Expected
        <pgx.PgError>: {
            Severity: "ERROR",
            Code: "42710",
            Message: "tablespace \"test_tablespace\" already exists",
```

## How to Contribute

See [CONTRIBUTING.md file](./CONTRIBUTING.md).

## License

Licensed under Apache License Version 2.0. For more details, please refer to
the [LICENSE](./LICENSE).

## Acknowledgment

Thanks to all the Greenplum Backup contributors, more details in its [GitHub
page](https://github.com/greenplum-db/gpbackup).