const express = require("express");
var cors = require("cors");
const bodyParser = require("body-parser");

const API_PORT = 3001;
const app = express();
app.use(cors());
const router = express.Router();

const MongoClient = require('mongodb').MongoClient
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true }, (err, client) => {
    if (err) return console.log(err)
    db = client.db('repo')
    app.listen(3001, () => {
        console.log('listening on 3001')
    })
})

app.get('/getCorrelation', (req, res) => {
    var cursor = db.collection('ruipang_zhou482.result').find().toArray(function(err, results) {
        res.send(results)
    })
})

