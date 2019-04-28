console.log("this works");
neighborhoods = JSON.parse(neighborhoods);
let map = L.map('map', {center: [42.3601, -71.0589], zoom: 15});
var current_neighborhood = null;
L.tileLayer('https://{s}.tile.openstreetmap.se/hydda/full/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: 'Tiles &copy; <a href="http://openstreetmap.se/" target="_blank">OpenStreetMap Sweden</a>' +
        ', Map &copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

geojson = L.geoJson(neighborhoods, {
    onEachFeature : $onEachFeature
});

function $onEachFeature(feature, layer) {
    layer.on("click", handleLayerClick);
}

geojson.addTo(map);



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