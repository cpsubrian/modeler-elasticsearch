#!/usr/bin/env node
var bench = require('bench')
  , stact = require('stact')
  , size = 1000;

// Create ES client.
var collection = require('../')({
  name: 'doodads',
  index: 'bench-' + Date.now()
});

var client = collection.client;

// Setup index.
client.createIndex(function (err) {
  if (err) throw err;
  client.exec({
    path: '/_cluster/health/' + collection.options.index,
    qs: {
      wait_for_status: 'yellow'
    }
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
      client.refreshIndex(function (err) {
        if (err) throw err;
        bench.runMain();
      });
    });
  });
});

// Things to compare...
exports.compare = {
  'REST': function (done) {
    collection.load(Math.floor(Math.random() * size), done);
  },
  'Memcached': function (done) {
    done();
  }
};

// Cleanup.
exports.done = function (data) {
  client.deleteIndex(function (err) {
    if (err) throw err;
    bench.show(data);
  });
};
