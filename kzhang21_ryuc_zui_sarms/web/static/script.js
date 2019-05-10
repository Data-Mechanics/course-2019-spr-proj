$(document).ready(function() {
    $("#zipCodes").change(function() {
        let zipCode = $(this)
            .children("option:selected")
            .val();
        alert(zipCode);
        $.ajax({
            type: "GET",
            url: "/yelpPlot",
            params: JSON.stringify({ zip: zipCode }),
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function(data) {
                console.log(data);
                if (data) {
                    var graphs = data;
                    Plotly.plot("chart", graphs, {});
                }
            },
            error: function(xhr) {
                // alert("error:");
                console.log(xhr);
            }
        });
    });
});
