assert = require('assert');
util = require('util');
modeler = require('../');

index = 'modeler_elasticsearch_test_' + Date.now();
client = new (require('../client'))({
  index: index,
  refresh: {
    index: true,
    update: true,
    delete: true
  }
});
extraOptions = {
  client: client
};

setUp = function (done) {
  client.createIndex(function (err) {
    if (err) return done(err);
    client.exec({
      path: '/_cluster/health/' + index,
      qs: {
        wait_for_status: 'yellow'
      }
    }, function (err) {
      if (err) return done(err);
      client.createMapping({
        type: 'apples',
        data: {
          apples: {
            _timestamp: {
              enabled: true
            }
          }
        }
      }, function (err) {
        if (err) return done(err);
        client.createMapping({
          type: 'oranges',
          data: {
            oranges: {
              _timestamp: {
                enabled: true
              }
            }
          }
        }, done);
      });
    });
  });
};

tearDown = function (done) {
  client.deleteIndex(done);
};
