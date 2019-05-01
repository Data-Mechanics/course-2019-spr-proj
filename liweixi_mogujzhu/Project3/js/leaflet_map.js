(function() {

  var map = L.map('map').setView([42.36, -71.03], 13);
  L.tileLayer('https://stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}.{ext}', {
    attribution: 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data &copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    subdomains: 'abcd',
    minZoom: 0,
    maxZoom: 20,
    ext: 'png'
  }).addTo(map);
    function onEachFeatureAlarm(feature, layer) {
        if (feature.properties && feature.properties.LOCATION) {
            layer.bindPopup(feature.properties.LOCATION);
        }
    }
    function onEachFeatureHydrants(feature, layer) {
        if (feature.properties && feature.properties.PLACEMENT_DATE_TIME) {
            layer.bindPopup(feature.properties.PLACEMENT_DATE_TIME);
        }
    }
    function onEachFeatureDepartment(feature, layer) {
        console.log(feature);
        if (feature.properties && feature.properties.LOCCONTACT && feature.properties.LOCNAME) {
            layer.bindPopup(feature.properties.LOCNAME + " " + feature.properties.LOCCONTACT);
        }
    }
  var marker_fire_alarm_boxes = {
    radius: 6,
    fillColor: "#ff7800",
    color: "#000",
    weight: 1,
    opacity: 1,
    fillOpacity: 0.8
  }
    var marker_fire_department = {
    radius: 15,
    fillColor: "#ff0000",
    color: "#000",
    weight: 1,
    opacity: 1,
    fillOpacity: 0.8
  }
    var marker_fire_hydrants = {
    radius: 3,
    fillColor: "#0000ff",
    color: "#000",
    weight: 1,
    opacity: 1,
    fillOpacity: 0.8
  }
  var fire_department_btn= document.getElementById('fire_department_btn');
  var fire_department_layer = L.geoJson(boston_fire_department_json, {
    pointToLayer: function (feature, latlng){
        return L.circleMarker(latlng, marker_fire_department);
    }
  },
  );
  fire_department_btn.addEventListener("click", function(){
    if ((map.hasLayer(fire_department_layer))){
        fire_department_layer.remove();
    }
    else fire_department_layer.addTo(map);
  });


  var fire_alarm_boxes_btn = document.getElementById('fire_alarm_boxes_btn');
  var fire_alarm_boxes_layer = L.geoJson(boston_fire_alarm_boxes_json, {
    pointToLayer: function (feature, latlng){
        return L.circleMarker(latlng, marker_fire_alarm_boxes);
    }
  },
  );
  fire_alarm_boxes_btn.addEventListener("click", function(){
    if (map.hasLayer(fire_alarm_boxes_layer)){
        fire_alarm_boxes_layer.remove();
    }
    else fire_alarm_boxes_layer.addTo(map);
  });


  var fire_hydrants_btn= document.getElementById('fire_hydrants_btn');
  console.log(fire_hydrants_btn);
  var fire_hydrants_layer = L.geoJson(boston_fire_hydrants_json, {
    pointToLayer: function (feature, latlng){
        return L.circleMarker(latlng, marker_fire_hydrants);
    },
  },
  );
  fire_hydrants_btn.addEventListener("click", function(){
    if (map.hasLayer(fire_hydrants_layer)){
        fire_hydrants_layer.remove();
    }
    else fire_hydrants_layer.addTo(map);
  });


})();