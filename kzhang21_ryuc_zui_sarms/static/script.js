$(document).ready(function(){
    let zipCode;
    $("#zipCodes").change(function(){
        zipCode = $(this).attr("value");
        $.ajax({
            type : 'POST',
            url : '/post',
            contentType: 'application/json;charset=UTF-8',
            data : {'data': zipCode},
            success: function (data) {
                if (data.success) {
                    alert(data.message);
                }
            },
            error: function (xhr) {
                alert('error');
            }
        });
    });
});