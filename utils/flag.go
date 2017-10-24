package utils

/*
 * This file contains functions and structs relating to flag parsing.
 */

import (
	"flag"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

/*
 * Functions for validating whether flags are set and in what combination
 */

func FlagIsSet(f *flag.Flag) bool {
	return (*f).Value.String() != (*f).DefValue
}

// Each flag passed to this function must be set
func CheckMandatoryFlags(flagNames ...string) {
	for _, name := range flagNames {
		f := flag.Lookup(name)
		if f == nil || !FlagIsSet(f) {
			logger.Fatal(errors.Errorf("Flag %s must be set", name), "")
		}
	}
}

// At most one of the flags passed to this function may be set
func CheckExclusiveFlags(flagNames ...string) {
	numSet := 0
	for _, name := range flagNames {
		f := flag.Lookup(name)
		if f != nil && FlagIsSet(f) {
			numSet++
		}
	}
	if numSet > 1 {
		logger.Fatal(errors.Errorf("No more than one of the following flags may be set: %s", strings.Join(flagNames, ", ")), "")
	}
}

type ArrayFlags []string

func (i *ArrayFlags) String() string {
	return strings.Join(*i, ", ")
}

func (i *ArrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

/*
 * Functions for validating flag values
 */

/*
 * Restoring a future-dated backup is allowed (e.g. the backup was taken in a
 * different time zone that is ahead of the restore time zone), so only check
 * format, not whether the timestamp is earlier than the current time.
 */
func IsValidTimestamp(timestamp string) bool {
	timestampFormat := regexp.MustCompile(`^([0-9]{14})$`)
	return timestampFormat.MatchString(timestamp)
}
