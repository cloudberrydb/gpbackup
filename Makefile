all: depend build test

SHELL := /bin/bash
.DEFAULT_GOAL := all
BACKUP=gpbackup
RESTORE=gprestore
DIR_PATH=$(shell dirname `pwd`)
BIN_DIR=$(shell echo $${GOPATH:-~/go} | awk -F':' '{ print $$1 "/bin"}')

GIT_VERSION := $(shell git describe --tags | awk -F "-" '{$$2+=0; print $$1 "." $$2}')
DEV_VERSION := $(shell git diff | wc -l | awk '{if($$1!=0) {print "+dev"}}')
BACKUP_VERSION_STR="-X github.com/greenplum-db/gpbackup/backup.version=$(GIT_VERSION)$(DEV_VERSION)"
RESTORE_VERSION_STR="-X github.com/greenplum-db/gpbackup/restore.version=$(GIT_VERSION)$(DEV_VERSION)"

DEST = .

GOFLAGS :=

.PHONY : coverage integration update_pipeline

dependencies :
		go get github.com/blang/semver
		go get github.com/jmoiron/sqlx
		go get github.com/lib/pq
		go get github.com/maxbrunsfeld/counterfeiter
		go get github.com/onsi/ginkgo/ginkgo
		go get github.com/onsi/gomega
		go get github.com/pkg/errors
		go get golang.org/x/tools/cmd/goimports
		go get gopkg.in/cheggaaa/pb.v1
		go get gopkg.in/DATA-DOG/go-sqlmock.v1
		go get gopkg.in/yaml.v2
		go get github.com/golang/lint/golint
		go get github.com/alecthomas/gometalinter

format :
		goimports -w .
		gofmt -w -s .

lint :
		! gofmt -l . | read
		gometalinter --config=gometalinter.config ./...

unit :
		ginkgo -r -randomizeSuites -noisySkippings=false -randomizeAllSpecs backup restore utils testutils 2>&1

integration :
		ginkgo -r -randomizeSuites -noisySkippings=false -randomizeAllSpecs integration 2>&1

test : lint unit integration

end_to_end : build
		./end_to_end/run.sh end_to_end/all_objects.sql
		./end_to_end/run.sh end_to_end/1k_tables_and_inherits.sql

coverage :
		@./show_coverage.sh

depend : dependencies

build :
		go build -tags '$(BACKUP)' $(GOFLAGS) -o $(BIN_DIR)/$(BACKUP) -ldflags $(BACKUP_VERSION_STR)
		go build -tags '$(RESTORE)' $(GOFLAGS) -o $(BIN_DIR)/$(RESTORE) -ldflags $(RESTORE_VERSION_STR)

build_linux :
		env GOOS=linux GOARCH=amd64 go build -tags '$(BACKUP)' $(GOFLAGS) -o $(BIN_DIR)/$(BACKUP) -ldflags $(BACKUP_VERSION_STR)
		env GOOS=linux GOARCH=amd64 go build -tags '$(RESTORE)' $(GOFLAGS) -o $(BIN_DIR)/$(RESTORE) -ldflags $(RESTORE_VERSION_STR)

build_mac :
		env GOOS=darwin GOARCH=amd64 go build -tags '$(BACKUP)' $(GOFLAGS) -o $(BIN_DIR)/$(BACKUP) -ldflags $(BACKUP_VERSION_STR)
		env GOOS=darwin GOARCH=amd64 go build -tags '$(RESTORE)' $(GOFLAGS) -o $(BIN_DIR)/$(RESTORE) -ldflags $(RESTORE_VERSION_STR)

install : all installdirs
		$(INSTALL_PROGRAM) gpbackup$(X) '$(DESTDIR)$(bindir)/gpbackup$(X)'

installdirs :
		$(MKDIR_P) '$(DESTDIR)$(bindir)'

clean :
		# Build artifacts
		rm -f $(BIN_DIR)/$(BACKUP)
		rm -f $(BIN_DIR)/$(RESTORE)
		# Test artifacts
		rm -rf /tmp/go-build*
		rm -rf /tmp/ginkgo*
		rm -rf /tmp/ginkgo*
		# Code coverage files
		rm -rf /tmp/cover*
		rm -rf /tmp/unit*

update_pipeline :
	fly -t gpdb set-pipeline -p gpbackup -c ci/pipeline.yml -l <(lpass show "Concourse Credentials" --notes)

push : format
	git pull -r && make test && git push
