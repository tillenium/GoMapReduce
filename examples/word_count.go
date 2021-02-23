package main

import (
	"gomr.com/gomr/mr"
	"strconv"
	"strings"
	"unicode"
)

/**
Map function. Detects words and return the value as the word as key and value as 1
*/

func Map(filename, content string) []mr.KeyValue {

	//detect word separator
	isWordSeparator := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(content, isWordSeparator)

	kva := []mr.KeyValue{}

	for _, w := range words {
		kv := mr.KeyValue{strings.ToLower(w), "1"}
		kva = append(kva, kv)
	}
	return kva
}

/**
Returns the number of occurences for the key
*/
func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
