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
        layers: {
            'candidate': true,
            'clusters': true
        },
        map: null
    }},
    methods: {
        getColorMap(){
            let list = []
            list.push("match")
            list.push(["get","ZIP5"])
            for (let zip in this.ziplist){
                list.push(this.ziplist[zip])
                list.push(this.getRandomColor())
            }
            list.push('#66ccff')
            return list
        },
        getRandomColor(){
            return '#'+convert.hsl.hex([
                140 + Math.random() * 30, 
                40 + Math.random() * 20, 
                37 + Math.random() * 40])
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
                "id": "candidate", 
                "source": "CAN", 
                "type": "symbol", 
                "layout": {
                    "icon-image": "marker-15",
                    "icon-size": 2.5,
                    "visibility": "visible"
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
                    "circle-color": [
                        "match",
                        ["get", "Cluster"],
                        0,"#F1C40F",
                        1,"#800080",
                        2,"#EC7063",
                        3,"#85C1E9",
                        4,"#F1C40F",
                        6,"#EC7063",
                        7,"#85C1E9",
                        8,"#F1C40F",
                        9,"#EC7063",
                        10,"#85C1E9",
                        12,"#A3E4D7",
                        13,"#EC7063",
                        15,"#85C1E9",
                        16,"#F1C40F",
                        17,"#EC7063",
                        19,"#85C1E9",
                        20,"#A3E4D7",
                        21,"#EC7063",
                        23,"#85C1E9",
                        24,"#F1C40F",
                        25,"#EC7063",
                        26,"#85C1E9",
                        27,"#F1C40F",
                        28,"#EC7063",
                        30,"#A3E4D7",
                        31,"#EC7063",
                        33,"#F1C40F",
                        34,"#85C1E9",
                        /*other*/ "#800080"
                    ]
            },
            "filter": ["==", "$type", "Point"],
        }); }); 
        this.map = map;
    }
}
</script>
<style lang="stylus">
    // #menu button
    //     font-size: 13px;
    //     color: #404040;
    //     display: block;
    //     margin: 0;
    //     padding: 0;
    //     padding: 10px;
    //     text-decoration: none;
    //     border-bottom: 1px solid rgba(0,0,0,0.25);
    //     text-align: center;
    //     &:last-child
    //         border none
    //     &:hover
    //         background #f8f8f8
    //         color #404040
    //     .active 
    //         background #3887be
    //         color #fff
    //         &:hover
    //             background #3074a4
 
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

