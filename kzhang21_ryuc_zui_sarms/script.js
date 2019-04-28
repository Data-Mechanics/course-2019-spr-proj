$(document).ready(function(){
    // Check Radio-box for rating
    $(".rating input:radio").attr("checked", false);

    $('.rating input').click(function () {
        $(".rating span").removeClass('checked');
        $(this).parent().addClass('checked');
    });

    $('input:radio').change(
      function(){
        var userRating = this.value;
    });

    //label for violation slider
    
});

function violValue(val) {
    document.getElementById('vioValue').value=val; 
}