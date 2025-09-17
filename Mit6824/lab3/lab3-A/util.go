package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func DPrintfln(prefix string, color string, format string, a ...interface{}) {
	if Debug {
		log.Printf(color+"%-5s "+ColorReset+format, append([]interface{}{prefix}, a...)...)
	}
}
