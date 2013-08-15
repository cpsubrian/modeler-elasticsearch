assert = require('assert');
util = require('util');
modeler = require('../');

index = 'modeler_elasticsearch_test_' + Date.now();
client = require('elasticsearch')({
  _index: index,
  request: require('elasticsearch-memcached'),
  server: {
    memcached: {
      host: 'localhost',
      port: 11211
    },
    rest: {
      host: 'localhost',
      port: 9200
    }
  }
});
extraOptions = {
  client: client,
  refresh: true
};

setUp = function (done) {
  client.indices.createIndex(function (err) {
    if (err) return done(err);
    client.cluster.health({
      _index: index,
      wait_for_status: 'yellow'
    }, function (err) {
      if (err) return done(err);
      client.indices.putMapping({_type: 'apples'}, {
        apples: {
          _timestamp: {
            enabled: true
          }
        }
      }, function (err) {
        if (err) return done(err);
        client.indices.putMapping({_type: 'oranges'}, {
          oranges: {
            _timestamp: {
              enabled: true
            }
          }
        }, done);
      });
    });
  });
};

tearDown = function (done) {
  client.indices.deleteIndex(done);
};
