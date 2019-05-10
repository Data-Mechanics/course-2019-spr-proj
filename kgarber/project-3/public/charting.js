
function changeToDuration() {
  console.log("Not ready yet...")
}

function changeToCount() {
  console.log("Not ready yet...")
}

function bestFitOn() {
  console.log("Not ready yet...")
}

function bestFitOff() {
  console.log("Not ready yet...") 
}

function round(val, decimalPts) {
  let tens = Math.pow(10, decimalPts)
  return Math.round(val * tens) / tens
}

function receivedRidesAndWeather(data, stats) {
  let
    padding = 30,
    width = 900 - 2 * padding,
    height = 500 - 2 * padding

  let
    xValue = d => d.tempAvg,
    yValue = d => d.numTrips,
    yValueDuration = d => d.avgDuration

  let 
    bestFit = stats.count["best-fit"],
    bestFitFunc = x => bestFit[0] * x + bestFit[1],
    bestFitDuration = stats.duration["best-fit"],
    bestFitDurationFunc = x => bestFitDuration[0] * x + bestFitDuration[1]

  let
    minTemp = d3.min(data, xValue),
    maxTemp = d3.max(data, xValue),
    tempRange = maxTemp - minTemp

  let
    minRides = d3.min(data, yValue),
    maxRides = d3.max(data, yValue),
    ridesRange = maxRides - minRides

  let
    minDuration = d3.min(data, yValueDuration),
    maxDuration = d3.max(data, yValueDuration),
    durationRange = maxDuration - minDuration

  let 
    xScale = d3.scaleLinear()
      .range([0, width])
      .domain([minTemp-10, maxTemp + 10]),
    xMap = d => xScale(xValue(d)),
    xAxis = d3.axisBottom().scale(xScale)

  let
    yScale = d3.scaleLinear()
      .range([height, 0])
      .domain([minRides-1500, maxRides+1000]),
    yMap = d => yScale(yValue(d)),
    yAxis = d3.axisLeft().scale(yScale)

  let
    yScaleDuration = d3.scaleLinear()
      .range([height, 0])
      .domain([minDuration-500, maxDuration-1500]),
    yMapDuration = d => yScaleDuration(yValueDuration(d)),
    yAxisDuration = d3.axisLeft().scale(yScaleDuration)

  let svg = d3.select("#bb-chart-area")
    .append("svg")
      .attr("width", width + 2 * padding)
      .attr("height", height + 2 * padding)
    .append("g")
      .attr("transform", "translate(" + padding + "," + padding + ")")

  let circles = svg.selectAll("circle")
    .data(data)
    .enter()
    .append("circle")
    .attr("cx", d => xMap(d))
    .attr("cy", d => yMap(d))
    .attr("r", 3)
    .attr("fill", "blue")

  let bestFitLine = svg.append("line")
    .attr("x1", d => xScale(minTemp))
    .attr("y1", d => yScale(bestFitFunc(minTemp)))
    .attr("x2", d => xScale(maxTemp))
    .attr("y2", d => yScale(bestFitFunc(maxTemp)))
    .attr("stroke", "maroon")
    .attr("stroke-width", 3)

  $("#correlation-field").html("Correlation: " + round(stats.count.correlation, 3))
  $("#p-val-field").html("P-Value: " + round(stats.count["p-value"], 3))


  //x axis
  svg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + (height - padding) + ")")
    .call(xAxis)
  svg.append("text")
    .attr("class", "label")
    .attr("x", width / 2)
    .attr("y", height)
    .style("text-anchor", "end")
    .text("Temperature (\u2109)");
  
  //y axis
  let yAxisSvg = svg.append("g")
    .attr("class", "y axis")  
    .attr("transform", "translate(" + (padding) + ", 0)")
    .call(yAxis)
  let yAxisDurationSvg = svg.append("g")
    .style("opacity", 0)
    .attr("class", "y axis")  
    .attr("transform", "translate(" + (padding) + ", 0)")
    .call(yAxisDuration)
  let yAxisLabel = svg.append("text")
    .attr("class", "label")
    .attr("transform", "rotate(-90)")
    .attr("x", -height/2 + 50)
    .attr("y", -10)
    .style("text-anchor", "end")
    .text("Ride Count");

  changeToDuration = function(){
    d3.select("#ride-count-button").classed("button-success", false)
    d3.select("#ride-duration-button").classed("button-success", true)
    let t = d3.transition().duration(1000);
    let t2 = d3.transition().duration(500);
    circles
      .transition(t)
      .attr("cy", d => yMapDuration(d))
      .attr("fill", "maroon")
    yAxisLabel
      .transition(t2)
      .style("opacity", 0)
      .transition(t2)
      .text("Ride Duration")
      .style("opacity", 1)
    yAxisSvg
      .transition(t)
      .style("opacity", 0)
    yAxisDurationSvg
      .transition(t)
      .style("opacity", 1)
    bestFitLine
      .transition(t)
      .attr("stroke", "blue")
      .attr("y1", d => yScale(bestFitDurationFunc(minTemp)))
      .attr("y2", d => yScale(bestFitDurationFunc(maxTemp)))
    $("#correlation-field").html("Correlation: " + round(stats.duration.correlation, 3))
    $("#p-val-field").html("P-Value: " + round(stats.duration["p-value"], 3))
  }

  changeToCount = function(){
    d3.select("#ride-count-button").classed("button-success", true)
    d3.select("#ride-duration-button").classed("button-success", false)
    let t = d3.transition().duration(1000);
    let t2 = d3.transition().duration(500);
    circles
      .transition(t)
      .attr("cy", d => yMap(d))
      .attr("fill", "blue")
    yAxisLabel
      .transition(t2)
      .style("opacity", 0)
      .transition(t2)
      .text("Ride Count")
      .style("opacity", 1)
    yAxisSvg
      .transition(t)
      .style("opacity", 1)
    yAxisDurationSvg
      .transition(t)
      .style("opacity", 0)
    bestFitLine
      .transition(t)
      .attr("stroke", "maroon")
      .attr("y1", d => yScale(bestFitFunc(minTemp)))
      .attr("y2", d => yScale(bestFitFunc(maxTemp)))
    $("#correlation-field").html("Correlation: " + round(stats.count.correlation, 3))
    $("#p-val-field").html("P-Value: " + round(stats.count["p-value"], 3))
  }

  bestFitOn = function() {
    let t2 = d3.transition().duration(250);
    d3.select("#best-fit-on").classed("button-success", true)
    d3.select("#best-fit-off").classed("button-error", false)
    bestFitLine
      .transition(t2)
      .style("opacity", 1)
  }

  bestFitOff = function() {
    let t2 = d3.transition().duration(250);
    d3.select("#best-fit-on").classed("button-success", false)
    d3.select("#best-fit-off").classed("button-error", true)  
    bestFitLine
      .transition(t2)
      .style("opacity", 0)
  }
}

function receivedNewStation(stations, newStation) {
  var mymap = L.map('mapid').setView([newStation.latitude, newStation.longitude], 13);
  L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox.streets',
    accessToken: 'pk.eyJ1Ijoia2VubnlnOTgiLCJhIjoiY2p2MW51cHBvMXdsMTN5cGZqcmlicHFoOSJ9.E_L84wwi-7IXva3Y0z2gDg'
  }).addTo(mymap);
  L.circle([newStation.latitude, newStation.longitude], {
    color: 'black',
    fillColor: 'red',
    fillOpacity: 1,
    radius: 100
  }).addTo(mymap);
  stations.forEach(st => {
    L.circle([
        st.location.geometry.coordinates[1], 
        st.location.geometry.coordinates[0]
      ], {
        color: 'black',
        fillColor: 'yellow',
        fillOpacity: 1,
        radius: 100
    }).addTo(mymap);
  })
}

function receivedBUStations(bu, nearby, common) {
  var mymap = L.map('mapid2').setView([
    bu.geometry.coordinates[1],
    bu.geometry.coordinates[0]
  ], 13.5);
  L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox.streets',
    accessToken: 'pk.eyJ1Ijoia2VubnlnOTgiLCJhIjoiY2p2MW51cHBvMXdsMTN5cGZqcmlicHFoOSJ9.E_L84wwi-7IXva3Y0z2gDg'
  }).addTo(mymap);
  // draw BU in red
  L.circle([
    bu.geometry.coordinates[1],
    bu.geometry.coordinates[0]
  ], {
    color: 'black',
    fillColor: 'red',
    fillOpacity: 0.3,
    radius: 500
  }).addTo(mymap);
  // draw nearby stations in green
  nearby.forEach(n => {
    L.circle([
      n.location.geometry.coordinates[1],
      n.location.geometry.coordinates[0]
    ], {
      color: 'black',
      fillColor: 'green',
      fillOpacity: 1,
      radius: 50
    }).addTo(mymap);
  })
  // draw common stations in orange
  common.forEach(n => {
    L.circle([
      n.location.geometry.coordinates[1],
      n.location.geometry.coordinates[0]
    ], {
      color: 'black',
      fillColor: 'orange',
      fillOpacity: 1,
      radius: 50
    }).addTo(mymap);
  })
}

$.get('/api/getData/bluebike,numrides', (data) => {
  receivedRidesAndWeather(data.data, data.stats[0])
})

$.get('/api/getData/bluebike,new-station', (data) => {
  receivedNewStation(data.stations, data["new_station"][0])
})

$.get('/api/getData/bluebike,BU', (data) => {
  receivedBUStations(data.BU, data.NEARBY, data.COMMON)
})

