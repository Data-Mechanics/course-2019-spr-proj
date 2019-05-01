(function() {

  var map = L.map('map').setView([42.36, -71.03], 13);
  L.tileLayer('https://stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}.{ext}', {
    attribution: 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data &copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    subdomains: 'abcd',
    minZoom: 0,
    maxZoom: 20,
    ext: 'png'
  }).addTo(map);
  function getColor(d) {
    return d > 5000 ? '#ff0000' :
      d > 4000 ? '#ee0000' :
      d > 2000 ? '#aa0000' :
      d > 1000 ? '#660000' :
      d > 100 ? '#330000' :
      d > 0 ? '#110000' :
      '#000000';
  }
  function style(feature) {
    return {
      "color": "blue",
      "fillColor": getColor(fire_incident_count[feature.properties.ZIP5]),
      "fillOpacity": 0.75,
      "weight": 3,
      "opacity": 1
    };
  }
  L.geoJson(zip_area, {style: style}).addTo(map);

})();