from conans import ConanFile, tools
import os


class GpbackupConan(ConanFile):
    name = "gpbackup"
    version = "1.0.0-alpha.1"
    license = "Apache License v2.0"
    url = "https://github.com/greenplum-db/gpbackup"
    description = "Greenplum DB backup and restore utilities"
    settings = "os", "compiler", "build_type", "arch"
    exports_sources = "src/*", "src/github.com/greenplum-db/gpbackup/.git/*"

    def build(self):
        os.environ["PATH"] = os.environ["PATH"] + ":" + os.path.join(os.getcwd(), "bin")
        with tools.environment_append({'GOPATH': os.getcwd()}):
            with tools.chdir('src/github.com/greenplum-db/gpbackup'):
                self.run('make build_linux')

    def package(self):
        self.copy("gpbackup", dst="bin", src="bin")
        self.copy("gprestore", dst="bin", src="bin")
        self.copy("gpbackup_helper", dst="bin", src="bin")
