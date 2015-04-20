package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	//"time"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

type Video struct {
	Title      string `json:title`
	Url        string `json:url`
	Poster     string `json:poster`
	DatePosted string `json:datePosted`
	CatVidId   int64  `json:catVidId`
	UpMeows    uint   `json:upMeows`
	DownMeows  uint   `json:downMeows`
}

func main() {
	//The db.sql object is meant to be long lived. It does not create a connection to the source
	//sql.Open create a connection to the db. instead it only prepares database abstraction for later use
	db, err := sql.Open("mysql", "jflewis:jflewis2015!@tcp(mysqlcs.millersville.edu:3306)/jflewis")

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("database connected to succesfully")
	}

	r := mux.NewRouter()
	//r.Get(name)
	r.HandleFunc("/randomVideo", getRandVid(db))
	r.HandleFunc("/getAllVideos", getAllVideos(db))
	r.HandleFunc("/addVideo", addVideo(db))
	r.HandleFunc("/getVideoByUser/{userId}", getVideoByUser(db))
	http.ListenAndServe(":8080", r)

}

func getRandVid(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		var video Video
		err := db.QueryRow(`SELECT CatVid.title,CatVid.url,CatVid.video_poster, 
		CatVid.date_posted ,CatVid.catVidID, Vote.upmeows, Vote.downmeows FROM CatVid,
		 Vote where Vote.catVidID = CatVid.catVidID ORDER BY RAND() LIMIT 1;`).Scan(&video.Title, &video.Url, &video.Poster, &video.DatePosted, &video.CatVidId, &video.UpMeows, &video.DownMeows)
		if err != nil {
			log.Fatal(err)
		}
		js, err := json.Marshal(video)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Access-Control-Allow-Origin", "*")

		rw.Write(js)
	}
}

func addVideo(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		title := req.PostFormValue("title")
		url := req.PostFormValue("url")
		tags := req.PostFormValue("tags")
		userName := req.PostFormValue("userName")
		endsWithComma := strings.HasSuffix(tags, ",")
		if !endsWithComma {
			tags += ","
		}
		_, err := db.Exec("call UploadVideo(?,?,?,?)", title, url, tags, userName)
		if err != nil {
			log.Fatal(err)
			http.Error(rw, err.Error(), http.StatusInternalServerError)
		}
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.WriteHeader(http.StatusCreated)

	}
}

func getVideoByUser(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		userId := vars["userId"]
		rows, err := db.Query(`SELECT CatVid.title,CatVid.url,CatVid.video_poster, CatVid.date_posted ,CatVid.catVidID, Vote.upmeows, Vote.downmeows FROM CatVid, Vote
   							where Vote.catVidID = CatVid.catVidID
  							 and CatVid.video_poster regexp ?;`, userId)
		defer rows.Close()
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		var videos []Video
		//iterate through result set and create a slice of videos
		for rows.Next() {
			var video Video
			err := rows.Scan(&video.Title, &video.Url, &video.Poster, &video.DatePosted, &video.CatVidId, &video.UpMeows, &video.DownMeows)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			videos = append(videos, video)
		}
		js, err := json.Marshal(videos)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.WriteHeader(http.StatusTeapot)
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.Write(js)

	}
}

func getAllVideos(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		rows, err := db.Query(`SELECT CatVid.title,CatVid.url,CatVid.video_poster, CatVid.date_posted ,CatVid.catVidID, Vote.upmeows, Vote.downmeows 
							FROM CatVid, Vote
   							where Vote.catVidID = CatVid.catVidID`)
		defer rows.Close()
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		var videos []Video
		//iterate through result set and create a slice of videos
		for rows.Next() {
			var video Video
			err := rows.Scan(&video.Title, &video.Url, &video.Poster, &video.DatePosted, &video.CatVidId, &video.UpMeows, &video.DownMeows)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			videos = append(videos, video)
		}
		//marshel slice of videos into a json array
		js, err := json.Marshal(videos)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.Write(js)
	}
}
