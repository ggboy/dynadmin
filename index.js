var config = require("./config.js").config();
var express = require('express');
var app = express();

app.use(express.static("public"));

app.use(function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

app.set('view engine', 'pug');

var server = app.listen(config.port, function() {
    var host = server.address().address;
    var port = server.address().port;
});

