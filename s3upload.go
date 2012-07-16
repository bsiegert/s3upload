package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"mime"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

var (
	nConn = flag.Int("n", 3, "maximum number of connections")
)

type Upload struct {
	fi os.FileInfo
	local, remote string
}

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
	if len(args) < 2 {
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

	conn := s3.New(auth, aws.EUWest)
	bucket := conn.Bucket(bucketname)

	// Start nConn upload routines
	var wg sync.WaitGroup
	wg.Add(*nConn)
	c := make(chan *Upload, *nConn)
	for i := 0; i < *nConn; i++ {
		go func() {
			for u := range c {
				f, err := os.Open(u.local)
				if err != nil {
					log.Print(err)
					continue
				}

				mimetype := mime.TypeByExtension(filepath.Ext(u.local))
				if mimetype == "" {
					mimetype = "application/octet-stream"
				}
				log.Print(u.remote)
				err = bucket.PutReader(u.remote, f, u.fi.Size(), mimetype, s3.PublicRead)
				if err == nil {
					f.Close()
					continue
				}
				// Infinite retries ...
				for i := 1; err != nil; i++ {
					log.Printf("Error uploading %s: %s", u.remote, err.Error())
					f.Seek(0, 0)
					log.Printf("%s (retry %d)", u.remote, i)
					err = bucket.PutReader(u.remote, f, u.fi.Size(), mimetype, s3.PublicRead)
				}
				f.Close()
			}
			wg.Done()
		}()
	}

	// Feed file names.
	for _, a := range args {
		u := &Upload{local: a}
		var err error

		u.fi, err = os.Stat(a)
		if err != nil {
			log.Printf("skipping %s: %s", a, err.Error())
			continue
		}
		if u.fi.IsDir() {
			basepath := filepath.Dir(a)
			err = filepath.Walk(a, func(p string, fi os.FileInfo, err error) error {
				if err == nil && !fi.IsDir() {
					u2 := &Upload{local: p, fi: fi}
					rel, err := filepath.Rel(basepath, p)
					if err != nil {
						return err
					}
					u2.remote = path.Join(bpath, filepath.ToSlash(rel))
					c <- u2
				}
				return nil
			})
			if err != nil {
				log.Print(err)
			}
		} else {
			u.remote = path.Clean(path.Join(bpath, u.fi.Name()))
			c <- u
		}
	}
	close(c)
	wg.Wait()
}
