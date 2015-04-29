$(document).ready( function() {

	var easter_egg = new Konami(function() { 
		$(".shrok").get(0).play(); 

		if ($(".cat-head") != null)
		{
			var angle = 0;
			setInterval(function(){
      		angle+=3;
     		$(".cat-head").rotate(angle);
			},10);
		}
	});

});