assert = require('assert');
util = require('util');
modeler = require('../');
idgen = require('idgen');

extraOptions = {};

tearDown = function (done) {
  // Delete entries from ES or something.
  done();
};