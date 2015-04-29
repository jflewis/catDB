//Load popular videos via catDB API
$.getJSON (api + "getAllVideos", function (data){
			showVideos('../', $("#videoContainer"),data);
		});

$("body").on ("click", ".toggle-video", function() {
	$(this).parent().nextAll('.expand-video').toggle();
});
