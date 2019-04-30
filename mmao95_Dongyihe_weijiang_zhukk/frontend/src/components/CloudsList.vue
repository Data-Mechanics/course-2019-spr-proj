<template>
    <section class="cloud-list card">
        <div class="card-body">
            <div class="row">
                <div class="col-5"><h3 class="card-title text-left">{{title}}</h3></div>
                <div class="col">
                    <div class="btn-toolbar float-right">
                        <div class="btn-group text-right mr-2" data-toggle="dropdown">
                            <button class="dropdown-toggle btn btn-secondary">{{keys[highlight]}}</button>
                            <div class="dropdown-menu dropdown-menu-right">
                                <button v-for="(list, key, index) in words"
                                data-toggle="list"
                                :key="key"
                                @click="switchKey(index)"
                                :class="'btn dropdown-item ' + (index == highlight ? 'active':'')"
                                :href="'#'+key"
                                >{{key}}</button>
                            </div>
                        </div>
                        
                        <div class="btn-group mr-2">
                            <button class="btn btn-secondary"
                                    :disabled="highlight==0"
                                    @click="last">Last</button>
                            <button class="btn btn-secondary"
                                    :disabled="highlight==keys.length-1"
                                    @click="next">Next</button>
                        </div>
                    </div>
                </div>
            </div>
            <div class="word-cloud" v-if="Object.keys(words).length>0">
                <WordCloud
                :words="words[keys[highlight]]"
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
import convert from 'color-convert'
export default {
    name: 'CloudsList',
    props: ['title', 'words', 'id'],
    watch: {
        words () {
            this.keys = Object.keys(this.words)
            this.switchKey(0)
        }
    },
    data() {return {
        highlight: 0,
        max: 1,
        keys: [],
        fontSizeMapper: word => Math.log2(word.value) * 5
    }},
    methods: {
        switchKey(i){
            this.highlight = i
            let key = this.keys[i]
            let max = 1
            this.words[key].forEach(([,weight])=>{
                max = weight > max ? weight : max
            })
            this.max = max
        },
        getColor([, weight]){
            return '#' + convert.hsl.hex([weight*35 + 94, 50 + Math.random()*20, 50 + Math.random()*20])
        },
        next(){
            if (this.highlight < this.keys.length - 1)
                this.switchKey(this.highlight + 1)
        },
        last(){
            if (this.highlight > 0)
                this.switchKey(this.highlight - 1)
        }
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

