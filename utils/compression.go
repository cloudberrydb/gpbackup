package utils

import "fmt"

var (
	compressionProgram Compression
)

type Compression struct {
	Name              string
	CompressCommand   string
	DecompressCommand string
	Extension         string
}

func InitializeCompressionParameters(compress bool, compressionLevel int) {
	if compress {
		compressionProgram = Compression{Name: "gzip", CompressCommand: fmt.Sprintf("gzip -c -%d", compressionLevel), DecompressCommand: "gzip -d -c", Extension: ".gz"}
	} else {
		compressionProgram = Compression{Name: "cat", CompressCommand: "cat -", DecompressCommand: "cat -", Extension: ""}
	}
}

func GetCompressionProgram() Compression {
	return compressionProgram
}

func SetCompressionProgram(compression Compression) {
	compressionProgram = compression
}
