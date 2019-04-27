<template>
    <section class="cloud-list card">
        <div class="card-body">
            <h3 class="card-title text-left">Rename Candidates</h3>
            <div class="row">
                <div class="col-10 word-cloud">
                    <WordCloud 
                    :words="words[highlight]"
                    :color="getColor"
                    font-family="Impact"/>
                </div>
                <div class="col">
                    <div class="dropdown" id="zipcodes" data-toggle="dropdown">
                        <button class="dropdown-toggle btn btn-secondary">{{highlight}}</button>
                        <div class="dropdown-menu">
                            <button v-for="(list, zipcode) in words"
                           data-toggle="list"
                           :key="zipcode"
                           @click="switchZipcode(zipcode)"
                           :class="'btn dropdown-item ' + (zipcode == highlight ? 'active':'')"
                           :href="'#zip-'+zipcode">{{zipcode}}</button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </section>
</template>
<script>
import WordCloud from 'vuewordcloud'
import axios from 'axios'
import convert from 'color-convert'
export default {
    name: 'CloudsList',
    created () {
        axios.get('http://localhost:5000/zipcode', {headers: {
	  'Access-Control-Allow-Origin': '*',
	}}).then(resp=>{
            this.words = resp.data
            this.switchZipcode(Object.keys(this.words)[0])
         })
    },
    data() {return {
        words: {
            12324: [['romance', 19], ['horror', 3], ['fantasy', 7], ['adventure', 3]],
            324234:[['roman', 219], ['horror', 13], ['fantasy', 7], ['adventure', 30]]
        },
        highlight: '',
        max: 1,
        min: -1,
        cloudData: [],
        fontSizeMapper: word => Math.log2(word.value) * 5
    }},
    methods: {
        switchZipcode(zipcode){
            this.highlight = zipcode
            // debugger
            let max = 1, min = -1
            this.words[zipcode].forEach(([,weight])=>{
                max = weight > max ? weight : max
                min = weight < min || min < 0 ? weight : min
            })
            this.max = max
            this.min = min
        },
        getColor([, weight]){
            return '#' + convert.hsl.hex([(weight-this.min)*200/(this.max-this.min) + 144 + Math.random()*10, 50, 50 + Math.random()*20])
        }
    },
    compute: {
    },
    components: {
        WordCloud
    }
}
</script>
<style lang="stylus">
.word-cloud
    height 600px
    padding 20px 100px
.dropdown-menu
    max-height 300px
    overflow-y scroll
</style>

