package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
	"database/sql"
	_"github.com/go-sql-driver/mysql"
)

// DomainURL represents a simple struct for JSON request parsing
type DomainURL struct {
	Domain string `json:"domain"`
	URL    string `json:"url"`
}

var (
	rdb     *redis.Client
	kafkaW  *kafka.Writer
	db      *sql.DB
	mongoDB *mongo.Collection
	ctx     = context.Background()
)

func main() {
	setupServices()
	http.HandleFunc("/post", postHandler)
	http.HandleFunc("/get", getHandler)
	http.ListenAndServe(":8080", nil) // Simplified without log.Fatal
}

//Really should try to go one by one
func setupServices() {
	// Setting up Redis
	rdb = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST") + ":6379",
	})

	// Setting up Kafka writer
	kafkaW = &kafka.Writer{
		Addr:     kafka.TCP(os.Getenv("KAFKA_HOST") + ":9092"),
		Topic:    "urls",
		Balancer: &kafka.LeastBytes{},
	}

	// Setting up MySQL
	db, _ = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/%s",
		os.Getenv("MYSQL_USER"), os.Getenv("MYSQL_PASSWORD"), os.Getenv("MYSQL_HOST"), os.Getenv("MYSQL_DB")))

	// Setting up MongoDB
	client, _ := mongo.Connect(ctx, options.Client().ApplyURI(
		fmt.Sprintf("mongodb://%s:%s@%s:27017", os.Getenv("MONGO_USER"), os.Getenv("MONGO_PASSWORD"), os.Getenv("MONGO_HOST"))))
	mongoDB = client.Database("mydb").Collection("urls")
}

//ToDo: try with gin
func postHandler(w http.ResponseWriter, r *http.Request) {
	var du DomainURL
	json.NewDecoder(r.Body).Decode(&du)

	rdb.Set(ctx, du.Domain, du.URL, 0)
	kafkaW.WriteMessages(ctx, kafka.Message{Key: []byte(du.Domain), Value: []byte(du.URL)})
	db.Exec("INSERT INTO urls (domain, url) VALUES (?, ?) ON DUPLICATE KEY UPDATE url = VALUES(url)", du.Domain, du.URL)
	mongoDB.ReplaceOne(ctx, bson.M{"domain": du.Domain}, bson.M{"domain": du.Domain, "url": du.URL}, options.Replace().SetUpsert(true))

	w.WriteHeader(http.StatusOK)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")
	url, _ := rdb.Get(ctx, domain).Result()
	w.Write([]byte(url))
}
