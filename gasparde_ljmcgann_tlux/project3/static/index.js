function postData(url = ``, data = {}) {
    // Default options are marked with *
    return fetch(url, {
        method: "POST", // *GET, POST, PUT, DELETE, etc.
        mode: "cors", // no-cors, cors, *same-origin
        cache: "no-cache", // *default, no-cache, reload, force-cache, only-if-cached
        credentials: "same-origin", // include, *same-origin, omit
        headers: {
            "Content-Type": "application/json",
            // "Content-Type": "application/x-www-form-urlencoded",
        },
        redirect: "follow", // manual, *follow, error
        referrer: "no-referrer", // no-referrer, *client
        body: JSON.stringify(data), // body data type must match "Content-Type" header
    })
        .then(response => response.json()); // parses JSON response into native Javascript objects
}

console.log("this works");
/*
    censushealth,
    kmeans,
    stats,
    neighborhoods,
    openspaces,
    parcelgeo,
    assessments,
    censusshape
 */
//console.log(censushealth);
// kmeans = JSON.parse(kmeans);
//console.log(kmeans);
//console.log(stats);
neighborhoods = JSON.parse(neighborhoods);
//console.log(neighborhoods);
// openspaces = JSON.parse(openspaces);
//console.log(openspaces);
//parcelgeo = JSON.parse(parcelgeo);
// console.log(parcelgeo);
// console.log(assessments);
censusshape = JSON.parse(censusshape);
console.log(censusshape);


let map = L.map('map', {center: [42.3601, -71.0589], zoom: 15});

let tile = L.tileLayer('https://{s}.tile.openstreetmap.se/hydda/full/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: 'Tiles &copy; <a href="http://openstreetmap.se/" target="_blank">OpenStreetMap Sweden</a>' +
        ', Map &copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

/*
L.tileLayer(
    'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    {maxZoom: 18}).addTo(map);
*/
let info = L.control();

info.onAdd = function (map) {
    this._div = L.DomUtil.create('div', 'info');
    this.update();
    return this._div;
};

info.update = function (props) {
    this._div.innerHTML = '<h4>Boston Neighborhoods</h4>' + (props ?
        '<b>' + props.Name
        : 'Hover over a Neighborhood');
};

info.addTo(map);

// get color depending on population density value
function getColor(d) {
    return d > 1000 ? '#800026' :
        d > 500 ? '#BD0026' :
            d > 200 ? '#E31A1C' :
                d > 100 ? '#FC4E2A' :
                    d > 50 ? '#FD8D3C' :
                        d > 20 ? '#FEB24C' :
                            d > 10 ? '#FED976' :
                                '#FFEDA0';
}

function style(feature) {
    return {
        weight: 2,
        opacity: 1,
        color: 'white',
        dashArray: '3',
        fillOpacity: 0.7,
        fillColor: getColor(feature.properties.Name)
    };
}

function highlightFeature(e) {
    let layer = e.target;

    layer.setStyle({
        weight: 5,
        color: '#666',
        dashArray: '',
        fillOpacity: 0.7
    });

    if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
        layer.bringToFront();
    }

    info.update(layer.feature.properties);
}

let neighborhoods_shape;
let censustract_shape;

function resetHighlight(e) {
    neighborhoods_shape.resetStyle(e.target);
    info.update();
}

function zoomToFeatureAndAddData(e) {
    map.fitBounds(e.target.getBounds());


    postData(`http://127.0.0.1:5000/`, {'neighborhood': e.target.feature.properties.Name})
        .then(data => console.log(JSON.stringify(data))) // JSON-string from `response.json()` call
        .catch(error => console.error(error));

}

function onEachFeature(feature, layer) {
    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
        click: zoomToFeatureAndAddData
    });
}
if (kmeans !== null) {
    console.log(typeof(kmeans));
    //kmeans = JSON.parse(kmeans);
    console.log(kmeans);
    for (let i = 0; i < kmeans.length; i++) {
        let marker = L.marker(kmeans[i]).addTo(map);
    }
    // kmeans.forEach(el => {
    //         for (let i = 0; i < el.means.length; i++) {
    //             let marker = L.marker(el.means[i]).addTo(map);
    //             marker.bindPopup("I am a mean calculate using the " + el.metric + " metric!");
    // });
}else {
    console.log("KMEANS not defined");
}

censustract_shape = L.geoJson(censusshape);
neighborhoods_shape = L.geoJson(neighborhoods, {
    style: style,
    onEachFeature: onEachFeature
});
//L.geoJson(parcelgeo).addTo(map);

let baseMaps = {
    "Map": tile
};
let overlayMaps = {
    "Census Tracts": censustract_shape,
    "Neighborhoods": neighborhoods_shape
};
L.control.layers(baseMaps, overlayMaps).addTo(map);
map.attributionControl.addAttribution('Population data &copy; <a href="http://census.gov/">US Census Bureau</a>');


let legend = L.control({position: 'bottomright'});

legend.onAdd = function (map) {

    let div = L.DomUtil.create('div', 'info legend'),
        grades = [0, 10, 20, 50, 100, 200, 500, 1000],
        labels = [],
        from, to;

    for (let i = 0; i < grades.length; i++) {
        from = grades[i];
        to = grades[i + 1];

        labels.push(
            '<i style="background:' + getColor(from + 1) + '"></i> ' +
            from + (to ? '&ndash;' + to : '+'));
    }

    div.innerHTML = labels.join('<br>');
    return div;
};

legend.addTo(map);
console.log("Finished Executing Script!");

function handleLayerClick(e) {
    var neighborhood = e.sourceTarget.feature.properties.Name;
    console.log(e.sourceTarget.feature.properties.Name);
    var node = document.querySelector("#neighborhood");
    node.innerHTML = neighborhood;
    current_neighborhood = neighborhood;
    var form = document.querySelector("#neighborhood_form");
    console.log(form);
    form.value =  neighborhood;

}