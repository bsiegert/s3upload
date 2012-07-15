package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

func ReadConfig() (accesskey string, secret string, err error) {
	homedir := os.Getenv("HOME")
	if homedir == "" {
		u, err := user.Current()
		if err != nil { return "", "", err }
		homedir = u.HomeDir
	}
	cfg, err := ioutil.ReadFile(homedir + "/.s3cfg")
	if err != nil { return }

	for off := 0; off != -1; off = bytes.IndexRune(cfg, '\n') {
		line := cfg[:off]
		cfg = cfg[off+1:]
		tokens := bytes.SplitN(line, []byte(" = "), 2)
		if len(tokens) != 2 {
			continue
		}
		switch string(tokens[0]) {
		case "access_key":
			accesskey = string(tokens[1])
		case "secret_key":
			secret = string(tokens[1])
		}
	}
	return
}

func main() {
	var err error
	var auth aws.Auth
	auth.AccessKey, auth.SecretKey, err = ReadConfig()
	if err != nil {
		log.Fatalf("Reading config file: %s", err.Error())
	}

	conn := s3.New(auth, aws.EUWest)
	bucket := conn.Bucket("miros")

	list, err := bucket.List("", "", "", 100)
	if err != nil {
		log.Fatal(err)
	}
	for _, item := range list.Contents {
		fmt.Printf("%s\t%d\n", item.Key, item.Size)
	}
}

