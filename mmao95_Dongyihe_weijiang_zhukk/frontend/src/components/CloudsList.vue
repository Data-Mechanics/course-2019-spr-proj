<template>
    <section class="cloud-list card">
        <div class="card-body">
            <div class="row">
                <div class="col-7"><h3 class="card-title text-left">{{title}}</h3></div>
                <div class="col">
                    <div class="dropdown text-right" data-toggle="dropdown">
                        <button class="dropdown-toggle btn btn-secondary">{{highlight}}</button>
                        <div class="dropdown-menu dropdown-menu-right">
                            <button v-for="(list, key) in words"
                            data-toggle="list"
                            :key="key"
                            @click="switchKey(key)"
                            :class="'btn dropdown-item ' + (key == highlight ? 'active':'')"
                            :href="'#'+key"
                            >{{key}}</button>
                        </div>
                    </div>
                </div>
            </div>
            <div class="word-cloud" v-if="Object.keys(words).length>0">
                <WordCloud
                :words="words[highlight]"
                :color="getColor"
                font-family="Impact"/>
            </div>
            <div class="word-cloud loading d-flex align-items-center justify-content-center" v-else>
                <h3 class="card-text">Loading...</h3>
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
    props: ['title', 'words', 'id'],
    watch: {
        words () {
            this.switchKey(Object.keys(this.words)[0])
        }
    },
    data() {return {
        highlight: '',
        max: 1,
        fontSizeMapper: word => Math.log2(word.value) * 5
    }},
    methods: {
        switchKey(key){
            this.highlight = key
            let max = 1
            this.words[key].forEach(([,weight])=>{
                max = weight > max ? weight : max
            })
            this.max = max
        },
        getColor([, weight]){
            return '#' + convert.hsl.hex([weight*220/this.max + 114, 50 + Math.random()*20, 50 + Math.random()*20])
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
.cloud-list
    margin 40px 0
.word-cloud
    height 600px
    padding 20px 80px
    &.loading
        background #eee
        color #ccc
        border-radius 20px
.dropdown-menu
    max-height 300px
    overflow-y scroll
    right 0
</style>

