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

// Width and Height of the whole visualization
var width = 700;
var height = 580;

// Create SVG
var svg = d3.select("div.mapid")
    .append("svg")
    .style("display", "block")
    .style("margin", "auto")
    .attr("width", width)
    .attr("height", height);

//Create a tooltip
var tooltip = d3.select("div.container#viol").append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);

// Append empty placeholder g element to the SVG
// g will contain geometry elements
var g = svg.append("g");

// Width and Height of the whole visualization
// Set Projection Parameters
var albersProjection = d3.geoAlbers()
    .scale(190000)
    .rotate([71.057, 0])
    .center([0, 42.313])
    .translate([width / 2, height / 2]);

// Create GeoPath function that uses built-in D3 functionality to turn
// lat/lon coordinates into screen coordinates
var geoPath = d3.geoPath()
    .projection(albersProjection);

g.selectAll("path")
    .data(neighborhoods_json.features)
    .enter()
    .append("path")
    .attr("fill", "#ccc")
    .attr("stroke", "#333")
    .on("mouseover", handleMouseOver_Violation)
    .on("mouseout", handleMouseOut_Violation)
    .attr("d", geoPath);

function handleMouseOver_Violation(d, i) {
    d3.select(this)
        .attr("stroke", "red")
        .attr("stroke-width", 3);

    tooltip.transition()
        .duration(200)
        .style("opacity", .9);

    tooltip.html(d.properties.Name)
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY - 28) + "px");
}

function handleMouseOut_Violation(d, i) {
    d3.select(this)
        .attr("fill", "#ccc")
        .attr("stroke", "#333")
        .attr("stroke-width", 1);

    tooltip.transition()
        .duration(500)
        .style("opacity", 0);
}


// // the points for restaurants
// var restaurants = svg.append("g");
//
// restaurants.selectAll("path")
//     .data(businesses)
//     .enter()
//     .append("path")
//     .attr("fill", "#900")
//     .attr("stroke", "#999")
//     .attr("d", geoPath);