// Width and Height of the whole visualization
var width = 700;
var height = 580;

var violation_data = {
    Allston: 0.0148305112,
    "Back Bay": 0.014673516,
    "Bay Village": 0.014673516,
    "Beacon Hill": 0.0079316101,
    Brighton: 0.013214928,
    Charlestown: 0.0111358323,
    Chinatown: 0.0142877959,
    "Leather District": 0.0142877959,
    Dorchester: 0.0200310624,
    Downtown: 0.0110069338,
    "East Boston": 0.0124370158,
    Fenway: 0.0148607211,
    Longwood: 0.0148607211,
    "Hyde Park": 0.0189047565,
    "Jamaica Plain": 0.0157670894,
    Mattapan: 0.0200543367,
    "Mission Hill": 0.0266863733,
    "North End": 0.0133527343,
    Roslindale: 0.0209122753,
    Roxbury: 0.0202292164,
    "South Boston": 0.0116010752,
    "South Boston Waterfront": 0.0146368075,
    "South End": 0.0187959066,
    "West End": 0.0187833246,
    "West Roxbury": 0.0201359561
};
var rating_data = {
    Allston: 3.6875,
    "Back Bay": 3.7775229358,
    "Bay Village": 3.7775229358,
    "Beacon Hill": 3.6938202247,
    Brighton: 3.7681818182,
    Charlestown: 3.6351351351,
    Chinatown: 3.6461864407,
    "Leather District": 3.6461864407,
    Dorchester: 3.4085106383,
    Downtown: 3.5961538462,
    "East Boston": 3.2379182156,
    Fenway: 3.5431937173,
    Longwood: 3.5431937173,
    "Hyde Park": 3.3333333333,
    "Jamaica Plain": 3.7987012987,
    Mattapan: 3.2878787879,
    "Mission Hill": 3.59375,
    "North End": 3.8477508651,
    Roslindale: 3.2448979592,
    Roxbury: 3.5316455696,
    "South Boston": 3.8106060606,
    "South Boston Waterfront": 3.5472972973,
    "South End": 3.8975903614,
    "West End": 3.7417582418,
    "West Roxbury": 3.6826923077
};

var colorScale = d3
    .scaleSequential(d3["interpolateBlues"])
    .domain([0.0079316101, 0.0200543367]);
// Create SVG
var svg = d3
    .select("div.mapid")
    .append("svg")
    .style("display", "block")
    .style("margin", "auto")
    .attr("width", width)
    .attr("height", height);

//Create a tooltip
var tooltip = d3
    .select("div.container#viol")
    .append("div")
    .attr("class", "tooltip")
    .style("opacity", 1);

// Append empty placeholder g element to the SVG
// g will contain geometry elements
var g = svg.append("g");

// Width and Height of the whole visualization
// Set Projection Parameters
var albersProjection = d3
    .geoAlbers()
    .scale(190000)
    .rotate([71.057, 0])
    .center([0, 42.313])
    .translate([width / 2, height / 2]);

// Create GeoPath function that uses built-in D3 functionality to turn
// lat/lon coordinates into screen coordinates
var geoPath = d3.geoPath().projection(albersProjection);

var neighborhoods_new = neighborhoods_json.features.filter(function(
    neighborhoods
) {
    return (
        neighborhoods.properties.Name != "Longwood Medical Area" &&
        neighborhoods.properties.Name != "Harbor Islands"
    );
});

g.selectAll("path")
    .data(neighborhoods_new)
    .enter()
    .append("path")
    .attr("fill", function(d) {
        if (!d.properties.Name) {
            return "#ccc";
        } else {
            return colorScale(violation_data[d.properties.Name]);
        }
    })
    .attr("stroke", "#333")
    .attr("fill-opacity", 0.7)
    .on("mouseover", handleMouseOver_Violation)
    .on("mouseout", handleMouseOut_Violation)
    .attr("d", geoPath);

var margin = { top: 20, right: 40, bottom: 30, left: 250 };

var axisScale = d3
    .scaleLinear()
    .domain(colorScale.domain())
    .range([margin.left, width - margin.right]);

var axisBottom = g =>
    g
        .attr("class", `x-axis`)
        .attr("transform", `translate(0,${height - margin.bottom})`)
        .call(
            d3
                .axisBottom(axisScale)
                .ticks(width / 120)
                .tickSize(-20)
        );

const linearGradient = g.append("linearGradient").attr("id", "linear-gradient");

linearGradient
    .selectAll("stop")
    .data(
        colorScale.ticks().map((t, i, n) => ({
            offset: `${(100 * i) / n.length}%`,
            color: colorScale(t)
        }))
    )
    .enter()
    .append("stop")
    .attr("offset", d => d.offset)
    .attr("stop-color", d => d.color);

g.append("g")
    .attr("transform", `translate(0,${height - margin.bottom - 20})`)
    .append("rect")
    .attr("transform", `translate(${margin.left}, 0)`)
    .attr("width", width - margin.right - margin.left)
    .attr("height", 20)
    .style("fill", "url(#linear-gradient)");

g.append("g").call(axisBottom);

function handleMouseOver_Violation(d, i) {
    d3.select(this)
        .attr("fill-opacity", 1)
        .attr("stroke-width", 2);

    tooltip
        .transition()
        .duration(200)
        .style("opacity", 0.9);

    tooltip
        .html(d.properties.Name + "<br>" + violation_data[d.properties.Name])
        .style("left", d3.event.pageX + "px")
        .style("top", d3.event.pageY - 28 + "px");
}

function handleMouseOut_Violation(d, i) {
    d3.select(this)
        .attr("fill-opacity", 0.7)
        .attr("stroke-width", 1);

    tooltip
        .transition()
        .duration(500)
        .style("opacity", 0);

    console.log("Mouse ", d, i);
    vm.neigh = d.properties["Name"];
}

//
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
