package utils

import (
	"gomr.com/gomr/mr"
	"log"
	"plugin"
)

/**
Loads the Map and Reduce functions from the given executing file.
It uses Plugin Library to parse and extract go functions from the executable.

input: filename of go executable with Map/Reduce functions
output:
 1. the Map function (string,string) -> []KeyValue
 2. Reduce Function (string, []string) -> string
*/
func LoadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)

	if err != nil {
		log.Fatalf( "cannot log plugin %v, err: %v" , filename, err)
	}

	xmapf, err := p.Lookup("Map")

	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}

	mapf := xmapf.(func(string, string) []mr.KeyValue)

	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}

	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef

}
