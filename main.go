package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strings"
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

type Comment struct {
	CommentId       int64  `json:commentId`
	CatVidId        int64  `json:catVidId`
	Poster          string `json:poster`
	ComentDesc      string `json:comentDesc`
	ParentCommentId *int64 `json:parentCommentId`
}

type Award struct {
	AwardId   int64  `json:awardId`
	AwardName string `json:awardName`
	AwardDesc string `json:awardName`
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
	s := r.Methods("PUT", "OPTIONS").Subrouter()
	q := r.Methods("POST").Subrouter()

	//routes
	r.HandleFunc("/randomVideo", getRandVid(db))
	r.HandleFunc("/getAllVideos", getAllVideos(db))
	r.HandleFunc("/getPopularVideos", getPopularVideos(db))
	r.HandleFunc("/getVideosByUser/{userId}", getVideoByUser(db))
	r.HandleFunc("/getVideosByTags", getVideoByTag(db))
	r.HandleFunc("/getComments/{catVidId}", getCommentsForVideo(db))
	r.HandleFunc("/getAwards", getAwards(db))
	r.HandleFunc("/getTags", getTags(db))
	r.HandleFunc("/getTags/{catVidId}", getTagsByVidId(db))
	//calls that need PUT
	s.HandleFunc("/upMeow/{catVidId}", upMeows(db))
	s.HandleFunc("/downMeow/{catVidId}", downMeows(db))
	s.HandleFunc("/addAward/{catVidId}/{awardId}", addAwardToVideo(db))
	//calls that POST
	q.HandleFunc("/addVideo", addVideo(db))
	q.HandleFunc("/postComment", postComment(db))

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
func addAwardToVideo(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		if req.Method == "OPTIONS" {
			rw.Header().Set("Access-Control-Allow-Methods", "PUT")
			rw.Header().Set("Access-Control-Allow-Origin", "*")
			return
		}
		vars := mux.Vars(req)
		catVidId := vars["catVidId"]
		awardId := vars["awardId"]
		_, err := db.Exec(`call AddAwardToVideo(?, ?) `, catVidId, awardId)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.WriteHeader(http.StatusNoContent)
		rw.Header().Set("Access-Control-Allow-Origin", "*")
	}
}

func getAwards(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		var awards []Award
		rows, err := db.Query(`SELECT * FROM Award;`)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var award Award
			err := rows.Scan(&award.AwardId, &award.AwardName, &award.AwardDesc)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			awards = append(awards, award)
		}
		js, err := json.Marshal(awards)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.Write(js)
	}
}

func upMeows(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		if req.Method == "OPTIONS" {
			rw.Header().Set("Access-Control-Allow-Methods", "PUT")
			rw.Header().Set("Access-Control-Allow-Origin", "*")
			return
		}
		vars := mux.Vars(req)
		catVidId := vars["catVidId"]

		_, err := db.Exec(`UPDATE Vote
   				SET upmeows = upmeows + 1
   				WHERE Vote.CatVidID = ?`, catVidId)

		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Access-Control-Allow-Methods", "PUT")
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.WriteHeader(http.StatusNoContent)

	}
}

func downMeows(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		if req.Method == "OPTIONS" {
			rw.Header().Set("Access-Control-Allow-Methods", "PUT")
			rw.Header().Set("Access-Control-Allow-Origin", "*")
			return
		}
		vars := mux.Vars(req)
		catVidId := vars["catVidId"]

		_, err := db.Exec(`UPDATE Vote
   				SET downmeows = downmeows + 1
   				WHERE Vote.CatVidID = ?`, catVidId)

		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Access-Control-Allow-Methods", "PUT")
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.WriteHeader(http.StatusNoContent)

	}
}

func addVideo(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		title := req.PostFormValue("title")
		url := req.PostFormValue("url")
		tags := req.PostFormValue("tags")
		userName := req.PostFormValue("userName")
		endsWithComma := strings.HasSuffix(tags, ",")
		if len(tags) == 0 {
			tags += "lazy paws,"
		}
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

func postComment(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		catVidId := req.PostFormValue("catVidId")
		poster := req.PostFormValue("userName")
		commentBody := req.PostFormValue("commentBody")
		parentCommentId := sql.NullString{req.PostFormValue("parentId"), true}
		if len(parentCommentId.String) == 0 {
			parentCommentId.Valid = false
		}

		_, err := db.Exec("call PostComment(?,?,?,?)", catVidId, parentCommentId, poster, commentBody)
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
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusAccepted)
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

func getPopularVideos(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		rows, err := db.Query(`
   				SELECT CatVid.title, CatVid.url, CatVid.video_poster, CatVid.date_posted, CatVid.catVidID, Vote.upmeows, Vote.downmeows
				FROM CatVid, Vote
				WHERE CatVid.CatVidID = Vote.CatVidID
				ORDER BY upmeows DESC 
				LIMIT 25`)
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

func getVideoByTag(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		tags := req.URL.Query().Get("tags")
		endsWithComma := strings.HasSuffix(tags, ",")
		if !endsWithComma {
			tags += ","
		}
		rows, err := db.Query(`SELECT CatVid.title,CatVid.url,CatVid.video_poster, CatVid.date_posted ,CatVid.catVidID, Vote.upmeows, Vote.downmeows FROM CatVid, Vote, Tag, VidTag
    where Vote.catVidID = CatVid.catVidID
    and  CatVid.catVidID = VidTag.catVidID
    and VidTag.tagName = Tag.tagName
    and find_in_set(Tag.tagName, ?)
    group by CatVid.catVidID;`, tags)
		defer rows.Close()

		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		var videos []Video

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

		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.Write(js)
	}
}

func getCommentsForVideo(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		catVidId := vars["catVidId"]

		rows, err := db.Query(`SELECT commentID, catVidID, postedBy, commentDesc, parentCommentID
						FROM Comments
						WHERE Comments.CatVidID = ?`, catVidId)
		defer rows.Close()

		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		var comments []Comment

		for rows.Next() {
			var c Comment
			err := rows.Scan(&c.CommentId, &c.CatVidId, &c.Poster, &c.ComentDesc, &c.ParentCommentId)

			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}

			comments = append(comments, c)
		}

		js, err := json.Marshal(comments)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.Write(js)
	}

}

func getTags(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {

		rows, err := db.Query("SELECT * FROM Tag;")
		defer rows.Close()
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		var tags []string
		//Store tags in a slice and marshal into json object
		for rows.Next() {

			var tagName string
			err := rows.Scan(&tagName)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}

			tags = append(tags, tagName)
		}

		js, err := json.Marshal(tags)

		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Allow-Access-Control-Origin", "*")
		rw.Write(js)
	}
}

func getTagsByVidId(db *sql.DB) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {

		vars := mux.Vars(req)
		catVid := vars["catVidId"]

		rows, err := db.Query(` SELECT Tag.tagName FROM Tag, VidTag
   where ? = VidTag.catVidID
   and VidTag.tagName = Tag.tagName;`, catVid)
		defer rows.Close()

		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		var vidTags []string
		for rows.Next() {
			var tag string
			err := rows.Scan(&tag)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
			}

			vidTags = append(vidTags, tag)
		}

		js, err := json.Marshal(vidTags)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Allow-Access-Control-Origin", "*")
		rw.Write(js)
	}
}
