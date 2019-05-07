<template>
    <section class="card">
        <div class="card-body">
            <h3 class="card-title text-left">Candidates in Map</h3>
                <div id="menu" class="list-group">
                <button v-for="(active, layer) in layers"
                        :key="layer"
                        :class="'list-group-item list-group-item-action ' + (active?'active':'')"
                        @click="click(layer, $event)"
                >{{layer}}</button>
            </div>
            <div id="map"></div>
        </div>
    </section>
</template>
<script>
import convert from 'color-convert'
import mapboxgl from 'mapbox-gl'
export default {
    name: 'MapViewer',
    data() {return {
        ziplist: ["02108", "02109", "02110", "02111", "02113", "02114", "02115", "02116", "02118", "02119", "02120", "02121", "02122", "02124", "02125", "02126", "02127", "02128", "02129", "02130", "02131", "02132", "02134", "02135", "02136", "02151", "02152", "02163", "02199", "02203", "02210", "02215", "02467"],
        zipdis: [["02108", "02114"], ["02109", "02110", "02113"], ["02111"], ["02115", "02215"], ["02116"], ["02118"], ["02119"], ["02120"], ["02121", "02122", "02124", "02125"], ["02126"], ["02127"], ["02128"], ["02129"], ["02130"], ["02131"], ["02132"], ["02134"], ["02135"], ["02136"], ["02151", "02152"], ["02163"], ["02199"], ["02203"], ["02210"], ["02467"]],
        layers: {
            'candidates(cluster)': true,
            'candidates(district)': false,
            'clusters': false
        },
        map: null
    }},
    methods: {
        getColorMap(){
            let list = []
            list.push("match")
            list.push(["get", "ZIP5"])
            for (let zip in this.zipdis) {
                for (let j in this.zipdis[zip]) {
                    var col = this.getRandomColor(140, 40, 37)
                    list.push(this.zipdis[zip][j])
                    list.push(col)
                }
            }
            list.push('#66ccff')
            return list
        },
        getClusterMap(){
            let list = []
            list.push("match")
            list.push(["get","Cluster"])
            var i
            for (i=0;i<34;i++){
                list.push(i)
                list.push(this.getRandomColor(30, 40, 37))
            }
            list.push('#66ccff')
            return list
        },
        getRandomColor(h, s, l){
            return '#'+convert.hsl.hex([
                h + Math.random() * 30, 
                s + Math.random() * 20, 
                l + Math.random() * 40])
        },
        click(layer, e){
            e.preventDefault();
            e.stopPropagation();
            var visibility = this.map.getLayoutProperty(layer, 'visibility');
            if (visibility === 'visible') {
                this.map.setLayoutProperty(layer, 'visibility', 'none'); 
                this.layers[layer] = false
            } else {
                this.map.setLayoutProperty(layer, 'visibility', 'visible');
                this.layers[layer] = true            
            }
        }
    },
    mounted(){
        mapboxgl.accessToken = 'pk.eyJ1IjoibW1hbzk1IiwiYSI6ImNqdXB0eHoxZjNlM200M25xZHgzMG82cDgifQ.Q2_T7ErCmRmP7Lpf26T1fg';
        let map = new mapboxgl.Map({
        container: 'map',
        style: 'mapbox://styles/mapbox/light-v10',
        zoom: 11,
        center: [-71.066, 42.325]
        });
        const colorMap = this.getColorMap();
        const clusterMap = this.getClusterMap();
        map.on('load', function() {
            map.addSource('SS', { 
                "type": "geojson", 
                "data": "/data/street.geojson"
            });
            map.addSource('ZIP', { 
                "type": "geojson", 
                "data": "/data/ZIP_Codes.geojson"
            });
            map.addSource('CAN', { 
                "type": "geojson", 
                "data": "/data/candidates.geojson"
            });
            map.addSource('CAND', { 
                "type": "geojson", 
                "data": "/data/can_district.geojson"
            });


            map.addLayer({ 
                "id": "zipcode", 
                "source": "ZIP", 
                "type": "fill", 
                "paint": {
                    "fill-color": colorMap,
                    "fill-opacity": 0.8
                }
            }); 
            map.addLayer({ 
                "id": "candidates(cluster)", 
                "source": "CAN", 
                "type": "symbol", 
                "layout": {
                    "icon-image": "marker-15",
                    "icon-size": 2.5,
                    "text-field": "{name}",
                    "text-font": ["Open Sans Semibold", "Arial Unicode MS Bold"],
                    "text-offset": [0, 0.6],
                    "text-anchor": "top",
                    "visibility": "visible"
                }
            });  
            map.addLayer({ 
                "id": "candidates(district)", 
                "source": "CAND", 
                "type": "symbol", 
                "layout": {
                    "icon-image": "marker-15",
                    "icon-size": 2.5,
                    "text-field": "{name}",
                    "text-font": ["Open Sans Semibold", "Arial Unicode MS Bold"],
                    "text-offset": [0, 0.6],
                    "text-anchor": "top",
                    "visibility": "none"
                }
            }); 
            map.addLayer({ 
                "id": "clusters", 
                "source": "SS", 
                "type": "circle",
                "layout": {
                    "visibility": "none"
                }, 
                "paint": { 
                    "circle-radius": 3,
                    "circle-color": clusterMap
            },
            "filter": ["==", "$type", "Point"],
            }); }); 
        this.map = map;
    }
}
</script>
<style lang="stylus"> 
    #menu
        position absolute
        right 0
        z-index 30
        margin 30px


    #map 
        height 600px
        canvas
            position relative!important
</style>

