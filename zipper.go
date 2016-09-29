package main

import (
    "archive/zip"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "os"
    "regexp"
    "strings"
    "time"

    "net/http"

    "github.com/AdRoll/goamz/aws"
    "github.com/AdRoll/goamz/s3"
    redigo "github.com/garyburd/redigo/redis"
)

type Configuration struct {
    AccessKey          string 
    SecretKey          string 
    Bucket             string 
    Region             string
    RedisServer        string
    RedisPort          string
    RedisPassword      string
}

var config = Configuration {
    AccessKey: os.Getenv("S3_KEY"),
    SecretKey: os.Getenv("S3_SECRET"),
    Bucket: os.Getenv("S3_BUCKET"),
    Region: os.Getenv("S3_REGION"),
    RedisServer: os.Getenv("REDIS_HOST"),
    RedisPort: os.Getenv("REDIS_PORT"),
    RedisPassword: os.Getenv("REDIS_PASSWORD"),
}

var aws_bucket *s3.Bucket
var redisPool *redigo.Pool

type RedisFile struct {
    FileName string
    Folder   string
    S3Path   string
}

func main() {
    initAwsBucket()
    InitRedis()

    fmt.Println("Running on port", os.Getenv("PORT"))
    http.HandleFunc("/", handler)
    http.ListenAndServe(":" + os.Getenv("PORT"), nil)
}

func initAwsBucket() {
    expiration := time.Now().Add(time.Hour * 1)
    auth, err := aws.GetAuth(config.AccessKey, config.SecretKey, "", expiration)

    if err != nil {
        panic(err)
    }

    aws_bucket = s3.New(auth, aws.GetRegion(config.Region)).Bucket(config.Bucket)
}

func InitRedis() {
    redisPool = &redigo.Pool{
        MaxIdle:     10,
        IdleTimeout: 1 * time.Second,
        Dial: func() (redigo.Conn, error) {
            c, err := redigo.Dial("tcp", strings.Join([] string {config.RedisServer, ":", config.RedisPort}, ""))

            if err != nil {
                return nil, err
            }

            if _, err := c.Do("AUTH", config.RedisPassword); err != nil {
                c.Close()
                return nil, err
            }

            return c, err
        },
        TestOnBorrow: func(c redigo.Conn, t time.Time) (err error) {
            if err != nil {
                panic("Error connecting to redis")
            }
            return
        },
    }
}

// Remove all other unrecognised characters apart from
var makeSafeFileName = regexp.MustCompile(`[#<>:"/\|?*\\]`)

func getFilesFromRedis(token string) (files []*RedisFile, err error) {
    redis := redisPool.Get()
    defer redis.Close()

    // Get the value from Redis
    result, err := redis.Do("GET", "zip:" + token)
    if err != nil {
        panic(err)
        return
    }

    if (result == nil) {
        panic(result)
        return
    }

    // Convert to bytes
    var resultByte []byte
    var ok bool
    if resultByte, ok = result.([]byte); !ok {
        return
    }

    // Decode JSON
    err = json.Unmarshal(resultByte, &files)
    if err != nil {
        panic("003")
        return
    }

    return
}

func handler(w http.ResponseWriter, r *http.Request) {
    start := time.Now()

    // Get "token" URL params
    tokens, ok := r.URL.Query()["token"]

    if !ok || len(tokens) < 1 {
        http.Error(w, "", 500)
        return
    }

    token := tokens[0]

    // Get 'as' parameter
    downloadAs, ok := r.URL.Query()["as"]

    if !ok && len(downloadAs) > 0 {
        downloadAs[0] = makeSafeFileName.ReplaceAllString(downloadAs[0], "")
        if downloadAs[0] == "" {
            downloadAs[0] = "download.zip"
        }
    } else {
        downloadAs = append(downloadAs, "download.zip")
    }

    files, err := getFilesFromRedis(token)

    if err != nil {
        panic("002")
        return
    }

    // Start processing the response
    w.Header().Add("Content-Disposition", "attachment; filename=\""+downloadAs[0]+"\"")
    w.Header().Add("Content-Type", "application/zip")

    // Loop over files, add them to the zip
    zipWriter := zip.NewWriter(w)
    for _, file := range files {
        if file.S3Path == "" {
            log.Printf("Missing path for file: %v", file)
            continue
        }

        // Build safe file file name
        safeFileName := makeSafeFileName.ReplaceAllString(file.FileName, "")

        if safeFileName == "" { // Unlikely but just in case
            safeFileName = "file"
        }

        // Read file from S3, log any errors
        rdr, err := aws_bucket.GetReader(file.S3Path)
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

        // Build a good path for the file within the zip
        zipPath := ""

        // Prefix folder name, if any
        if file.Folder != "" {
            zipPath += file.Folder
            if !strings.HasSuffix(zipPath, "/") {
                zipPath += "/"
            }
        }

        zipPath += safeFileName

        h := &zip.FileHeader {
            Name:   zipPath,
            Method: zip.Deflate,
        }

        f, _ := zipWriter.CreateHeader(h)

        io.Copy(f, rdr)
        rdr.Close()
    }

    zipWriter.Close()

    log.Printf("%s\t%s\t%s", r.Method, r.RequestURI, time.Since(start))
}
