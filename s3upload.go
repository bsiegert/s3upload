package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"strings"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

func ParseURL(url string) (bucket string, path string) {
	if len(url) < 6 || url[0:5] != "s3://" {
		return "", ""
	}
	s := strings.SplitN(url[5:], "/", 2)
	return s[0], s[1]
}

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
	flag.Parse()

	var err error
	var auth aws.Auth
	auth.AccessKey, auth.SecretKey, err = ReadConfig()
	if err != nil {
		log.Fatalf("Reading config file: %s", err.Error())
	}

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		return
	}
	na := len(args)
	target := args[na-1]
	args = args[:na-1]

	bucketname, bpath := ParseURL(target)
	if bucketname == "" {
		log.Fatalf("invalid target URL: %s", target)
	}
	log.Print(bucketname, bpath)

	conn := s3.New(auth, aws.EUWest)
	bucket := conn.Bucket(bucketname)

	list, err := bucket.List(bpath, "", "", 100)
	if err != nil {
		log.Fatal(err)
	}
	for _, item := range list.Contents {
		fmt.Printf("%s\t%d\n", item.Key, item.Size)
	}
}

