#!/usr/bin/env node
var bench = require('bench')
  , stact = require('stact')
  , redis = require('redis').createClient()
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
  client: require('elasticsearch-memcached')({
    _index: index,
  })
});
var rcollection = require('modeler-redis')({
  name: 'doodads',
  prefix: index,
  client: redis
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
      var data = this;
      process.stdout.write('.');
      collection.create(data, function (err) {
        if (err) return next(err);
        rcollection.create(data, next);
      });
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
  },
  'redis': function (done) {
    rcollection.load(Math.floor(Math.random() * size), done);
  }
};

// Cleanup.
exports.done = function (data) {
  client.indices.refresh(function (err) {
    if (err) throw err;
    client.indices.deleteIndex({_index: index}, function (err) {
      if (err) throw err;
      redis.keys(index + ':*', function (err, keys) {
        if (err) throw err;
        redis.del(keys, function (err) {
          if (err) throw err;
          bench.show(data);
        });
      });
    });
  });
};
