neighborhoods = JSON.parse(neighborhoods);

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
function getColor(p) {
    if (typeof p.Color != 'undefined') {
        return p.Color;
    } else {
        return "#0000ff"
    }
}

function style(feature) {
    return {
        weight: 2,
        opacity: 1,
        color: 'white',
        dashArray: '3',
        fillOpacity: 0.7,
        fillColor: getColor(feature.properties)
    };
}

function highlightFeature(e) {
    let layer = e.target;
    layer.setStyle({
        weight: 5,
        color: '#8e9399',
        dashArray: '',
        fillOpacity: 0.7,
        fillColor: getColor(layer.feature.properties)
    });
    if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
        layer.bringToFront();
    }
    info.update(layer.feature.properties);
}

let neighborhoods_shape;

function resetHighlight(e) {
    neighborhoods_shape.resetStyle(e.target)
    info.update();
}

function onEachFeature(feature, layer) {
    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
        click: handleLayerClick,
    });
}

if (kmeans !== null) {
    kmeans = JSON.parse(kmeans);
    console.log(kmeans);
    let list_means = JSON.parse(kmeans["kmeans"]);
    for (let i = 0; i < list_means.length; i++) {
        var marker = L.marker(list_means[i]).addTo(map);
        var popup = L.popup().setContent("<h5>Cost per Square Feet: $" + kmeans["Avg_Land_Val"][i].toString() + "</h5>"
            + "<h5>Closest Park: " + kmeans["Dist_To_Park"][i].toString() + "km</h5>"
        + "<h5>Avg Health Score: " + kmeans["Avg_Health"][i].toString() + "</h5>");
        marker.bindPopup(popup).openPopup();
    }
}
neighborhoods_shape = L.geoJson(neighborhoods, {
    style: style,
    onEachFeature: onEachFeature
}).addTo(map);


map.attributionControl.addAttribution('Population data &copy; <a href="http://census.gov/">US Census Bureau</a>');
let go_back = L.control({position: "bottomleft"})

function goback(map) {
    let button = L.DomUtil.create("button", "Back");
    button.innerHTML = "Back";
    button.onclick = resetColor;
    return button;
}

go_back.onAdd = goback;
formfocus = (id) => {
    document.getElementById(id).focus();
};

function handleLayerClick(e) {
    map.fitBounds(e.target.getBounds());
    var neighborhood = e.sourceTarget.feature.properties.Name;
    formfocus('num_kmeans');
    current_neighborhood = neighborhood;
    var form = document.querySelector("#neighborhood_form");
    form.value = neighborhood;
    resetColor(e);
    e.target.feature.properties.Color = "#ff0000";
    neighborhoods_shape.resetStyle(e.target);
    go_back.addTo(map);

}

if (name !== null) {
    for (l in neighborhoods_shape._layers) {
        if (neighborhoods_shape._layers[l].feature.properties.Name === name) {
            map.fitBounds(neighborhoods_shape._layers[l].getBounds());
            neighborhoods_shape._layers[l].feature.properties.Color = "#ff0000";
            neighborhoods_shape.resetStyle(neighborhoods_shape._layers[l]);
        }
    }
}

function resetColor(e) {

    for (l in neighborhoods_shape._layers) {
        neighborhoods_shape._layers[l].feature.properties.Color = "#0000ff";
        neighborhoods_shape.resetStyle(neighborhoods_shape._layers[l]);
    }
}