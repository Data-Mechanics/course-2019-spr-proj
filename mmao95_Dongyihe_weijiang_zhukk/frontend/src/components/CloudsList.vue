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
                    <div class="list-group" id="zipcodes">
                        <button v-for="(list, zipcode) in words"
                           data-toggle="list"
                           :key="zipcode"
                           @click="switchZipcode(zipcode)"
                           :class="'list-group-item list-group-item-action ' + (zipcode == highlight ? 'active':'')"
                           :href="'#zip-'+zipcode">{{zipcode}}</button>
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
        // axios.get('/words').then(resp=>{ this.words = resp.body })
        this.switchZipcode(Object.keys(this.words))
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
            return '#' + convert.hsl.hex([(weight-this.min)*200/(this.max-this.min) + 144, 50, 50])
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
</style>

