function postData(url = ``, data = {}) {
    // Default options are marked with *
    console.log("this works");
    console.log(data);
    return fetch(url, {
        method: "POST", // *GET, POST, PUT, DELETE, etc.
        mode: "cors", // no-cors, cors, *same-origin
        cache: "no-cache", // *default, no-cache, reload, force-cache, only-if-cached
        credentials: "include", // include, *same-origin, omit
        headers: {
            "Content-Type": "application/json",
            // "Content-Type": "application/x-www-form-urlencoded",
        },
        redirect: "follow", // manual, *follow, error
        referrer: "no-referrer", // no-referrer, *client
        body: JSON.stringify(data), // body data type must match "Content-Type" header
    })
        .then(response => console.log(response)/*response.json()*/); // parses JSON response into native Javascript objects

}

/*

document.forms['myFormId'].addEventListener('submit', (event) => {
    event.preventDefault();
    // TODO do something here to show user that form is being submitted
    let form = new FormData(event.target);
    // fetch( 'http://127.0.0.1:5000/'/!*event.target.action*!/, {
    //     method: 'POST',
    //     body: new URLSearchParams(new FormData(event.target)) // event.target is the form
    // }).then((resp) => {
    //     return resp.json(); // or resp.text() or whatever the server sends
    // }).then((body) => {
    //     // TODO handle body
    //     console.log(body)
    // }).catch((error) => {
    //     console.log("rrrrrrrrrrrrrrrrrrrrreeeeeeeeeeeeeeeeeeeeeeeee");
    // });
    postData(event.target.action, {
        num_means: form.get('kmeans'),
        neighborhood: form.get('neighborhood'),
        weight: form.get('weight')
    })
        .then(data => console.log(JSON.stringify(data))) // JSON-string from `response.json()` call
        .catch(error => console.error(error));
});
*/


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
//kmeans = JSON.parse(kmeans);
//console.log(kmeans);
//console.log(stats);
neighborhoods = JSON.parse(neighborhoods);
//name = name.toString();
//console.log(neighborhoods);
// openspaces = JSON.parse(openspaces);
//console.log(openspaces);
//parcelgeo = JSON.parse(parcelgeo);
// console.log(parcelgeo);
// console.log(assessments);
//censusshape = JSON.parse(censusshape);
//console.log(censusshape);


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
function getColor(p) {
    if (typeof p.Color != 'undefined') {
        return p.Color;
    } else {
        return "#7fc9f4"
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
        color: '#666',
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
let censustract_shape;

function resetHighlight(e) {
    neighborhoods_shape.resetStyle(e.target)
    info.update();
}

function zoomToFeatureAndAddData(e) {
    map.fitBounds(e.target.getBounds());
}

function onEachFeature(feature, layer) {
    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
        click: handleLayerClick,
    });
}

if (kmeans !== null) {
    console.log(typeof (kmeans));
    kmeans = JSON.parse(kmeans);
    console.log(kmeans);
    for (let i = 0; i < kmeans.length; i++) {
        let marker = L.marker(kmeans[i]).addTo(map);
    }
    // kmeans.forEach(el => {
    //         for (let i = 0; i < el.means.length; i++) {
    //             let marker = L.marker(el.means[i]).addTo(map);
    //             marker.bindPopup("I am a mean calculate using the " + el.metric + " metric!");
    // });
} else {
    console.log("KMEANS not defined");
}



//censustract_shape = L.geoJson(censusshape);
neighborhoods_shape = L.geoJson(neighborhoods, {
    style: style,
    onEachFeature: onEachFeature
}).addTo(map);
//L.geoJson(parcelgeo).addTo(map);

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
    e.target.feature.properties.Color = "#382ac1";
    neighborhoods_shape.resetStyle(e.target);
    go_back.addTo(map);

}

if (name !== null) {
    console.log(typeof (name));
    //kmeans = JSON.parse(kmeans);
    name = name.toString();
    console.log(name);
    console.log(neighborhoods_shape);
    for (l in neighborhoods_shape._layers) {
        if (neighborhoods_shape._layers[l].feature.properties.Name === name) {
            map.fitBounds(neighborhoods_shape._layers[l].getBounds());
            neighborhoods_shape._layers[l].feature.properties.Color = "#382ac1";
            neighborhoods_shape.resetStyle(neighborhoods_shape._layers[l]);
        }
    }
} else {
    console.log("name not defined");
}

function resetColor(e) {

    for (l in neighborhoods_shape._layers) {
        neighborhoods_shape._layers[l].feature.properties.Color = "#7fc9f4";
        neighborhoods_shape.resetStyle(neighborhoods_shape._layers[l]);
    }
}