all: depend build test

SHELL := /bin/bash
.DEFAULT_GOAL := all
BACKUP=gpbackup
RESTORE=gprestore
DIR_PATH=$(shell dirname `pwd`)

DEST = .

GOFLAGS :=
dependencies :
		go get github.com/jmoiron/sqlx
		go get github.com/lib/pq
		go get github.com/maxbrunsfeld/counterfeiter
		go get github.com/onsi/ginkgo/ginkgo
		go get github.com/onsi/gomega
		go get github.com/pkg/errors
		go get golang.org/x/tools/cmd/goimports
		go get gopkg.in/DATA-DOG/go-sqlmock.v1

format :
		goimports -w .
		go fmt ./...

unit :
		ginkgo -r -randomizeSuites -randomizeAllSpecs backup restore utils testutils 2>&1

integration :
		ginkgo -r -randomizeSuites -randomizeAllSpecs integration 2>&1

test : unit integration

depend : dependencies

build :
		go build -tags '$(BACKUP)' $(GOFLAGS) -o ../../bin/$(BACKUP)
		go build -tags '$(RESTORE)' $(GOFLAGS) -o ../../bin/$(RESTORE)

build_rhel :
		env GOOS=linux GOARCH=amd64 go build -tags '$(BACKUP)' $(GOFLAGS) -o ../../bin/$(BACKUP)
		env GOOS=linux GOARCH=amd64 go build -tags '$(RESTORE)' $(GOFLAGS) -o ../../bin/$(RESTORE)

build_osx :
		env GOOS=darwin GOARCH=amd64 go build -tags '$(BACKUP)' $(GOFLAGS) -o ../../bin/$(BACKUP)
		env GOOS=darwin GOARCH=amd64 go build -tags '$(RESTORE)' $(GOFLAGS) -o ../../bin/$(RESTORE)

install : all installdirs
		$(INSTALL_PROGRAM) gpbackup$(X) '$(DESTDIR)$(bindir)/gpbackup$(X)'

installdirs :
		$(MKDIR_P) '$(DESTDIR)$(bindir)'

clean :
		rm -f $(BACKUP)
		rm -rf /tmp/go-build*
		rm -rf /tmp/ginkgo*

update_pipeline :
	fly -t gpdb set-pipeline -p gpbackup -c ci/pipeline.yml -l <(lpass show "Concourse Credentials" --notes)

push : format
	git pull -r && make test && git push

.PHONY : update_pipeline integration
