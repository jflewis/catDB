$(document).ready( function() {

    $('#post-video-form').submit(function(e) {

    e.preventDefault(); //STOP default action

    console.log("submitted");

    var postData = $(this).serializeArray();

    $.ajax({
        type: 'POST',
        url: 'http://floyd.cs.millersville.edu:8080/addVideo',
        data: postData,
        dataType: 'json',
        complete: function(json) {
            alert("Hey! Purr-fect video!");
            window.location.href = "../index.html";
          }
        });
    });
});