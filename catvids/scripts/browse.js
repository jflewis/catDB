var api = "http://localhost:8080";

function getPopularVideos (appendToId) {
		$.getJSON (api+"/getPopularVideos", function (data) {
			$.each (data, function (key, val) {

				var voteContainer = '<div class="video-link" id="' + val.CatVidId + '">\
									 <div class="vote-container">\
									 <span class="upmeow-text">' + val.UpMeows + '</span>\
									 <img class="upmeow" title="UpMeow this post! Purr!" src="../img/Giant-Cat-Head-1.jpg" height="30" width="30">\
									 <img class="downmeow" title="DownMeow this post! Hiss!" src="../img/Giant-Cat-Head-1.jpg" height="30" width="30">\
									 <span class="downmeow-text">' + val.DownMeows + '</span></div>';

				var vidTitle = '<span class="post-hover">' + val.Title + '</span>';

				var vidPostedBy = '<span class="posted-by"> Posted by: ' + val.Poster + '</span></div>'

				var embed = getId(val.Url);

				var vidLiElements = '<li class="expand-video">\
										<iframe width="560" height="315" src="https://www.youtube.com/embed/' + embed + '" frameborder="0" allowfullscreen></iframe>\
									</li>';
				
				var vidCommentsLink = '<li class="comment-li">\
											<a href="../comment/index.html?vidId=' + val.CatVidId + '">Comment on this video</a>\
										</li>';

				$(appendToId).append('<ul id="video-list"><li>' + voteContainer + vidTitle + vidPostedBy + vidCommentsLink + '</li></ul>');

				$("#"+val.CatVidId).one("click", ".post-hover", function() {
					$(this).parent().parent().append(vidLiElements);
					$(this).addClass("toggle-video");
				});

				$("#"+val.CatVidId).one("click", ".upmeow", function() {
	  				$(this).rotate({animateTo:360});
	  				$(this).parent().find(".upmeow-text").css("background-color", "orange");
	  				var catVidId = $(this).parent().parent().attr('id');
	 				upMeowVideo(catVidId);
	 				//add 1 to upmeow
	  				var newVote = Number($(this).parent().find(".upmeow-text").text()) + 1;
	  				$(this).parent().find(".upmeow-text").text(newVote);			
				});

				$("#"+val.CatVidId).one("click", ".downmeow", function() {
	  				$(this).rotate({animateTo:180});
	  				$(this).nextAll(".downmeow-text").css("background-color", "cyan");
	  				var catVidId = $(this).parent().parent().attr('id');
	  				downMeowVideo(catVidId);
	  				//add 1 to downmeow
	  				var newVote = Number($(this).parent().find(".downmeow-text").text()) + 1;
	  				$(this).parent().find(".downmeow-text").text(newVote);
				});
		});
	});

}

function getId(url) {
    var regExp = /^.*(youtu.be\/|v\/|u\/\w\/|embed\/|watch\?v=|\&v=)([^#\&\?]*).*/;
    var match = url.match(regExp);

    if (match && match[2].length == 11) {
        return match[2];
    } else {
        return 'error';
    }
}

function upMeowVideo(catVidId) {	
	$.ajax({
    	url: api +"/upMeow/" + catVidId,
    	method: "PUT",
    	//crossDomain : true,
    	success: function(result) {
    		//turn off other option
    		$("#"+catVidId).off("click", ".downmeow");
    	}
    });
}	

function downMeowVideo(catVidId) {
	$.ajax({
    	url: api + "/downMeow/" + catVidId,
    	type: 'PUT',
    	success: function(result) {
    		//turn off other option
    		$("#"+catVidId).off("click", ".upmeow");
    	}
    });
}

//Load popular videos via catDB API
getPopularVideos($("#videos-wrapper"));	

$("body").on ("click", ".toggle-video", function() {
	$(this).parent().nextAll('.expand-video').toggle();
});
