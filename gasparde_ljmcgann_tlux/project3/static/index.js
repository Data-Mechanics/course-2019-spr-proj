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
console.log(kmeans);
//console.log(stats);
neighborhoods = JSON.parse(neighborhoods);
//console.log(neighborhoods);
openspaces = JSON.parse(openspaces);
kmeans = JSON.parse(kmeans);
//console.log(openspaces);
// console.log(parcelgeo);
// console.log(assessments);
// console.log(censusshape);


let map = L.map('map', {center: [42.3601, -71.0589], zoom: 15});

L.tileLayer('https://{s}.tile.openstreetmap.se/hydda/full/{z}/{x}/{y}.png', {
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

let geojson;

function resetHighlight(e) {
    geojson.resetStyle(e.target);
    info.update();
}

function zoomToFeatureAndAddData(e) {
    map.fitBounds(e.target.getBounds());
    L.geoJSON(openspaces, {
        filter: function (feature, layer) {
            console.log(feature.properties.Name,e.target.feature.properties.Name);
            return feature.properties.Name === e.target.feature.properties.Name;
        }
  }).addTo(map);
}

function onEachFeature(feature, layer) {
    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
        click: zoomToFeatureAndAddData
    });
}

geojson = L.geoJson(neighborhoods, {
    style: style,
    onEachFeature: onEachFeature
}).addTo(map);

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
var print_kmeans = function (kmeans){
    var node = document.querySelector(".container");
    var k = node.createElement("p");
    k.appendChild(document.createTextNode(kmeans));
}
print_kmeans(kmeans);
console.log("Finished Executing Script!");