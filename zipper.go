package main

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
)

type config struct {
	awsAccessKey  string
	awsSecretKey  string
	awsBucket     string
	awsRegion     string
	redisServer   string
	redisPort     string
	redisPassword string
	srvPort       string
}

type server struct {
	pool         *redis.Pool
	bucket       *s3.Bucket
	safeFileName *regexp.Regexp
}

type redisFile struct {
	S3Path   string
	FileName string
	Folder   string
}

func main() {
	cfg := config{
		awsAccessKey:  os.Getenv("S3_KEY"),
		awsSecretKey:  os.Getenv("S3_SECRET"),
		awsBucket:     os.Getenv("S3_BUCKET"),
		awsRegion:     os.Getenv("S3_REGION"),
		redisServer:   os.Getenv("REDIS_HOST"),
		redisPort:     os.Getenv("REDIS_PORT"),
		redisPassword: os.Getenv("REDIS_PASSWORD"),
		srvPort:       os.Getenv("PORT"),
	}

	pool := initRedisPool(cfg)

	bkt, err := initS3Bucket(cfg)
	if err != nil {
		log.Fatalln(err)
	}

	srv := &server{
		pool:         pool,
		bucket:       bkt,
		safeFileName: regexp.MustCompile(`[#<>:"/\|?*\\]`),
	}

	http.HandleFunc("/", srv.handler)

	if cfg.srvPort == "" {
		cfg.srvPort = "8080"
	}
	http.ListenAndServe(":"+cfg.srvPort, nil)
}

func initS3Bucket(cfg config) (*s3.Bucket, error) {
	exp := time.Now().Add(time.Hour)
	auth, err := aws.GetAuth(cfg.awsAccessKey, cfg.awsSecretKey, "", exp)
	if err != nil {
		return nil, err
	}

	rgn, ok := aws.Regions[cfg.awsRegion]
	if !ok {
		return nil, errors.New("Region not found")
	}

	return s3.New(auth, rgn, aws.RetryingClient).Bucket(cfg.awsBucket), nil
}

func initRedisPool(cfg config) *redis.Pool {
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", cfg.redisServer+":"+cfg.redisPort)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", cfg.redisPassword); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		MaxIdle:     10,
		IdleTimeout: 1 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return pool
}

func (srv *server) handler(w http.ResponseWriter, r *http.Request) {
	strt := time.Now()

	qry := r.URL.Query()
	if len(qry) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	token, ok := qry["token"]
	if !ok || len(token[0]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	as, ok := qry["as"]
	if !ok {
		as = append(as, "download.zip")
	}
	if ok {
		as[0] = srv.safeFileName.ReplaceAllString(as[0], "")
	}
	if len(as[0]) == 0 {
		as[0] = "download.zip"
	}

	files, err := getRedisFiles(token[0], srv.pool)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	w.Header().Add("Content-Disposition", "attachment; filename=\""+as[0]+"\"")
	w.Header().Add("Content-Type", "application/zip")

	zw := zip.NewWriter(w)
	defer zw.Close()
	for _, file := range files {
		if file.S3Path == "" {
			log.Printf("Missing path for file: %v", file)
			continue
		}

		safeFileName := srv.safeFileName.ReplaceAllString(file.FileName, "")

		if safeFileName == "" {
			safeFileName = "file"
		}

		rdr, err := srv.bucket.GetReader(file.S3Path)
		if err != nil {
			switch t := err.(type) {
			case *s3.Error:
				if t.StatusCode == 404 {
					log.Printf("File not found. %s", file.S3Path)
				}
			default:
				log.Printf("Error downloading \"%s\" - %s", file.S3Path, err.Error())
			}
			continue
		}

		zipPath := ""

		if file.Folder != "" {
			zipPath += file.Folder
			if !strings.HasSuffix(zipPath, "/") {
				zipPath += "/"
			}
		}

		zipPath += safeFileName

		h := &zip.FileHeader{
			Name:   zipPath,
			Method: zip.Deflate,
		}

		f, _ := zw.CreateHeader(h)

		io.Copy(f, rdr)
		rdr.Close()
	}

	log.Printf("%s\t%s\t%s", r.Method, r.RequestURI, time.Since(strt))
}

func getRedisFiles(tkn string, pool *redis.Pool) ([]redisFile, error) {
	conn := pool.Get()
	defer conn.Close()

	rply, err := conn.Do("GET", "zip:"+tkn)
	if err != nil {
		return nil, err
	}
	if rply == nil {
		return nil, errors.New("No files found")
	}

	var (
		b  []byte
		ok bool
	)
	if b, ok = rply.([]byte); !ok {
		return nil, errors.New("Assertion failed")
	}

	var files []redisFile
	if err := json.Unmarshal(b, &files); err != nil {
		return nil, err
	}
	return files, nil
}
