package protocol

import (
	"unicode"
	"unicode/utf8"
)

const (
	MaxDisplayNameBytes = 256
	MaxDirectoryBytes   = 4096
)

func ValidDisplayMetadata(name, directory string) bool {
	for _, field := range []struct {
		value string
		limit int
	}{{name, MaxDisplayNameBytes}, {directory, MaxDirectoryBytes}} {
		if len(field.value) > field.limit || !utf8.ValidString(field.value) {
			return false
		}
		for _, r := range field.value {
			if unicode.IsControl(r) {
				return false
			}
		}
	}
	return true
}
