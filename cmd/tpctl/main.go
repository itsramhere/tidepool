// Command tpctl is an administrative CLI for Tidepool: it issues development
// JWTs and inspects documents and node state.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/itsramhere/tidepool/internal/auth"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "doc":
		if err := docCmd(os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "token":
		tokenCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "tpctl: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tpctl <command> [flags]")
	fmt.Fprintln(os.Stderr, "  doc get [--url http://127.0.0.1:7101] [--token ...] cards/c1")
	fmt.Fprintln(os.Stderr, "  token   issue a development JWT (--secret --uid --ttl)")
}

func tokenCmd(args []string) {
	fs := flag.NewFlagSet("tpctl token", flag.ExitOnError)
	secret := fs.String("secret", "", "JWT signing secret (required)")
	uid := fs.String("uid", "", "user id for the sub claim (required)")
	ttl := fs.Duration("ttl", 24*time.Hour, "token validity duration")
	_ = fs.Parse(args)
	if *secret == "" || *uid == "" {
		fmt.Fprintln(os.Stderr, "tpctl token: --secret and --uid are required")
		fs.Usage()
		os.Exit(2)
	}
	token, err := auth.Issue(*secret, *uid, *ttl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tpctl token: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(token)
}
