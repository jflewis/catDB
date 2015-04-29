$(document).ready(function() {

/*************EVENT HANDLERS**************************************************************/	
	//cat spinning fun
	$(".search-button").one("click", function(event) {
		var angle = 0;
		setInterval(function(){
      		angle+=3;
     		$(".cat-head").rotate(angle);
		},10);
	});

	//for search by user
	$("#submit-search-byUser").on("click", function(event){
		event.preventDefault();
		var userName = $("body").find("#searchbar").val();
		$.getJSON (api + "getVideosByUser/" + userName, function (data){
			showVideos("", $("#videoContainer"),data);
		});
		//getVideos($("#videoContainer"),"/getVideosByUser/"+userName,"what")
	});

	//for search by tags
	$("#submit-search-tags").on("click", function(event){
		event.preventDefault();
		var userTags = $("body").find("#searchbar").val();
		$.getJSON (api + "getVideosByTags",{tags: userTags }, function (data){
			showVideos("", $("#videoContainer"),data);
		});
	});

	//for all videos
	$("#submit-allVideos").on("click", function(event){
		event.preventDefault();
		$.getJSON (api + "getAllVideos", function (data){
			showVideos("", $("#videoContainer"),data);
		});
	});
	
	$("#submit-random").on("click", function(event) {
		event.preventDefault();
		$("#videoContainer").empty();
		$.getJSON( api + "randomVideo", function ( data ) {
			showSingleVideo ("", $("#videoContainer"), data);
		});
	});

/****************************************************************************************/
});