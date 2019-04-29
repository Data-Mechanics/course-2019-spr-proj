<template>
  <div id="app">

    <!-- <HelloWorld msg="Welcome to Your Vue.js App"/> -->
    <Header/>
    <div class="container">
    <CloudsList title="Candidates by Neighbourhoods" :words="neighbourhood" id="neighbouthood"/>
    <CloudsList title="Candidates by K-Means Cluster" :words="cluster" id="cluster"/>
    </div>
    <Footer/>
  </div>
</template>

<script>
import Header from './components/Header.vue'
import CloudsList from './components/CloudsList'
import Footer from './components/Footer'
import axios from 'axios'
export default {
  name: 'app',
  components: {
    Header,
    CloudsList,
    Footer
  },
  created () {
    let config = {headers: {'Access-Control-Allow-Origin': '*'}}
    axios.get('http://localhost:5000/neighbourhood', config).then(resp=>{
          this.neighbourhood = resp.data
    })
    axios.get('http://localhost:5000/cluster', config).then(resp=>{
          this.cluster = resp.data
    })
  },
  data(){return{
    neighbourhood: {},
    cluster: {}
  }}
}
</script>
<style lang="stylus">
#app
  font-family 'Avenir', Helvetica, Arial, sans-serif
  -webkit-font-smoothing antialiased
  -moz-osx-font-smoothing grayscale
  text-align center
  color #2c3e50
</style>
