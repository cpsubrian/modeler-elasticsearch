#!/usr/bin/env node
var bench = require('bench')
  , stact = require('stact')
  , size = 1000
  , index = 'bench-' + Date.now();

// Create ES clients.
var collection = require('../')({
  name: 'doodads',
  client: require('elasticsearch')({
    _index: index,
  })
});
var mcollection = require('../')({
  name: 'doodads',
  client: require('elasticsearch')({
    _index: index,
  })
});

var client = collection.client;

// Setup index.
client.indices.createIndex(function (err) {
  if (err) throw err;
  client.cluster.health({
    _index: index,
    wait_for_status: 'yellow'
  }, function (err) {
    if (err) throw err;

    // Insert a bunch of models.
    console.log('Creating models ');
    var stack = stact(function (next) {
      process.stdout.write('.');
      collection.create(this, next);
    });
    for (var i=0; i<size; i++) {
      stack.add({id: i});
    }
    stack.runSeries(function (err) {
      if (err) throw err;
      console.log('');
      client.indices.refresh(function (err) {
        if (err) throw err;
        bench.runMain();
      });
    });
  });
});

// Things to compare...
exports.compare = {
  'elasticsearch': function (done) {
    collection.load(Math.floor(Math.random() * size), done);
  },
  'elasticsearch-memcache': function (done) {
    mcollection.load(Math.floor(Math.random() * size), done);
  }
};

// Cleanup.
exports.done = function (data) {
  client.indices.refresh(function (err) {
    client.indices.deleteIndex({_index: index}, function (err) {
      if (err) throw err;
      bench.show(data);
    });
  });
};
