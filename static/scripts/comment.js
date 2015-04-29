$(document).ready(function(){

	//grab the url query string and parse it for the Vid ID
	var pathname = $(location).attr("search");
	var vidId = pathname.substring(pathname.lastIndexOf("=")+1);
	if (vidId == -1 || vidId == ""){
		console.log("Hey some shit went wrong I guess.");
	
	
	}else{
	var video = getVideoById(vidId);
	document.title = video.Title + " " + document.title;
	$("#video-title").text(video.Title);
	}

	loadAllComments();

function loadAllComments() {
	$.ajax({
		url: api + "getComments/" + vidId,
		async: false,
		dataType: 'json',
		success: function(data){
			commentJSON = data;
			console.log(commentJSON);
			var to_append = "";

			if (commentJSON != null)
			{
				commentJSON.forEach (function (e) {
					console.log(e);
					to_append += "<div class='meme'>Posted by user: " + e.Poster + "<ul class='comment-ul'><li class='comment-li'>" + e.CommentDesc + "</li></ul></div>";
			
			});
			}
			
			$("#comment-chain").append(to_append);

		}
	});

}


function getVideoById(vidId){
	var vidJSON;

	$.ajax({
		url: api + "getVideoByVidId/" + vidId,
		async:false,
		dataType: 'json',
		success: function(data){
			vidJSON = data;
			showSingleVideo('../',$("#video-wrapper"),vidJSON);

		}
	});

	return vidJSON;

	
}
$("#comment-form").submit(function(e) {
	e.preventDefault();
	$("#catvidID").val(vidId);
	var postData = $(this).serializeArray();
	$.ajax({
        type: 'POST',
        url: api + "postComment",
        data: postData,
        dataType: 'json',
        complete: function(json) {
         	$("#comment").val("");
         	$("#userName").val("");
          }
        });
    });
});