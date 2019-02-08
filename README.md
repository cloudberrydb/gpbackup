# Greenplum Backup

`gpbackup` and `gprestore` are Go utilities for performing Greenplum Database backups.  They are still currently in active development.

## Pre-Requisites

The project requires the Go Programming language version 1.8 or higher. Follow the directions [here](https://golang.org/doc/) for installation, usage and configuration instructions.

## Downloading

```bash
go get github.com/greenplum-db/gpbackup/...
```

This will place the code in `$GOPATH/github.com/greenplum-db/gpbackup`.

## Building and installing binaries

Make the `gpbackup` directory your current working directory and run:

```bash
make depend
make build
```

The `build` target will put the `gpbackup` and `gprestore` binaries in `$HOME/go/bin`.

This will also attempt to copy `gpbackup_helper` to the greenplum segments (retrieving hostnames from `gp_segment_configuration`). Pay attention to the output as it will indicate whether this operation was successful.

`make build_linux` and `make build_mac` are for cross compiling between macOS and Linux

`make install_helper` will scp the `gpbackup_helper` binary (used with -single-data-file flag) to all hosts

## Validation and code quality

### Test setup

Required for Greenplum Database 6 or higher, several tests require the `dummy_seclabel` Greenplum contrib module. This module exists only to support regression testing of the SECURITY LABEL statement. It is not intended to be used in production. Use the following commands to install the module.

```bash
pushd ~/workspace/gpdb/contrib/dummy_seclabel
    make install
    gpconfig -c shared_preload_libraries -v dummy_seclabel
    gpstop -ra
    gpconfig -s shared_preload_libraries | grep dummy_seclabel
popd

```

### Test execution

**NOTE**: The integration and end_to_end tests require a running Greenplum Database instance.

To run all tests except end-to-end (linters, unit, and integration), use
```bash
make test
```
To run only unit tests, use
```bash
make unit
```
To run only integration tests (requires a running GPDB instance), use
```bash
make integration
```

To run end to end tests (requires a running GPDB instance), use
```bash
make end_to_end
```

**We provide the following targets to help developers ensure their code fits Go standard formatting guidelines.**

To run a linting tool that checks for basic coding errors, use
```bash
make lint
```
This target runs [gometalinter](https://github.com/alecthomas/gometalinter).

Note: The lint target will fail if code is not formatted properly.


To automatically format your code and add/remove imports, use
```bash
make format
```
This target runs [goimports](https://godoc.org/golang.org/x/tools/cmd/goimports) and [gofmt](https://golang.org/cmd/gofmt/).
We will only accept code that has been formatted using this target or an equivalent `gofmt` call.

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

## Cleaning up

To remove the compiled binaries and other generated files, run
```bash
make clean
```

# More Information

The Greenplum Backup [wiki](https://github.com/greenplum-db/gpbackup/wiki) for this project has several articles providing a more in-depth explanation of certain aspects of gpbackup and gprestore.

# How to Contribute

We accept contributions via [Github Pull requests](https://help.github.com/articles/using-pull-requests) only.

Follow the steps below to contribute to gpbackup:
1. Fork the project’s repository.
1. Run `go get github.com/greenplum-db/gpbackup/...` and add your fork as a remote.
1. Run `make depend` to install required dependencies
1. Create your own feature branch (e.g. `git checkout -b gpbackup_branch`) and make changes on this branch.
    * Follow the previous sections on this page to setup and build in your environment.
    * Add new tests to cover your code. We use [Ginkgo](http://onsi.github.io/ginkgo/) and [Gomega](https://onsi.github.io/gomega/) for testing.
1. Run `make format`, `make test`, and `make end_to_end` in your feature branch and ensure they are successful.
1. Push your local branch to the fork (e.g. `git push <your_fork> gpbackup_branch`) and [submit a pull request](https://help.github.com/articles/creating-a-pull-request).

Your contribution will be analyzed for product fit and engineering quality prior to merging.
Note: All contributions must be sent using GitHub Pull Requests.

**Your pull request is much more likely to be accepted if it is small and focused with a clear message that conveys the intent of your change.**

Overall we follow GPDB's comprehensive contribution policy. Please refer to it [here](https://github.com/greenplum-db/gpdb#contributing) for details.

# Troubleshooting

## Dummy Security Label module is not installed or configured

If you see errors in many integration tests (below), review the
Validation and code quality [Test setup](#Test setup) section above:

```
SECURITY LABEL FOR dummy ON TYPE public.testtype IS 'unclassified';
      Expected
          <pgx.PgError>: {
              Severity: "ERROR",
              Code: "22023",
              Message: "security label provider \"dummy\" is not loaded",
```

## Tablespace already exists

If you see errors indicating the `test_tablespace` tablespace already
exists (below), execute `psql postgres -c 'DROP TABLESPACE
test_tablespace'` to cleanup the environment and rerun the tests.

```
    CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'
    Expected
        <pgx.PgError>: {
            Severity: "ERROR",
            Code: "42710",
            Message: "tablespace \"test_tablespace\" already exists",
```
