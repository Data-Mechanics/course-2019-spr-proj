<template>
  <div id="app">

    <!-- <HelloWorld msg="Welcome to Your Vue.js App"/> -->
    <Header/>
    <div class="container">
    <CloudsList/>
    </div>
    <Footer/>
  </div>
</template>

<script>
import Header from './components/Header.vue'
import CloudsList from './components/CloudsList'
import Footer from './components/Footer'
export default {
  name: 'app',
  components: {
    Header,
    CloudsList,
    Footer
  }
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
