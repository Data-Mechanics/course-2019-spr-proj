function getCrashJSON(){
    var crashData = new array();
    $.getJSON( "json/crash_cord.json", function( data ) {
        console.log(data);
        $.each(data, function(key,value){
            crashData.append({"lat":value["x"], "lng":value['y']});
        })
    });
    return crashData;
}
