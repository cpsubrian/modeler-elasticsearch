var formatUrl = require('url').format
  , request = require('request')
  , retry = require('retry');

module.exports = ElasticSearchClient;

function ElasticSearchClient(options) {
  var self = this;

  options = options || {};

  self._host = options.host || 'localhost';
  self._port = options.port || 9200;
  self._auth = options.port || false;
  self._protocol = options.protocol || 'http';
  self._index = options.index;
  self._retry = options.retry || {
    retries: 5,
    minTimeout: 50,
    maxTimeout: 3000
  };

  options.refresh = options.refresh || {};
  self._refresh = {
    index: options.refresh.index || false,
    update: options.refresh.update || false,
    delete: options.refresh.delete || false
  };
}

ElasticSearchClient.prototype.search = function (options, cb) {
  var path = '/' + (options.index || this._index);
  if (options.type) {
    path += '/' + options.type;
  }
  path += '/_search';
  var data = options.data || {};
  this.exec({path:path, method: 'POST', data: data, qs: options.qs}, cb);
};

ElasticSearchClient.prototype.get = function (options, cb) {
  var path = '/' + (options.index || this._index);
  path += '/' + options.type + '/' + options.id;
  this.exec({path: path, method: 'GET', data: options.data}, cb);
};

ElasticSearchClient.prototype.index = function (options, cb) {
  var self = this;
  var path = '/' + (options.index || this._index);
  path += '/' + options.type;
  var method = 'POST';
  if (options.id) {
    path += '/' + options.id;
    method = 'PUT';
  }
  this.exec({path: path, method: method, data: options.data}, function (err, results) {
    if (err) return cb(err);
    if (self._refresh.index || options.refresh) {
      self.refreshIndex(options, function (err, _result) {
        cb(err, results);
      });
    }
    else {
      cb(err, results);
    }
  });
};

ElasticSearchClient.prototype.update = function (options, cb) {
  var self = this;
  var path = '/' + (options.index || this._index);
  path += '/' + options.type + '/' + options.id + '/_update';
  this.exec({path: path, method: 'POST', data: options.data, qs: {retry_on_conflict: 10}}, function (err, results) {
    if (err) return cb(err);
    if (self._refresh.update || options.refresh) {
      self.refreshIndex(options, function (err) {
        cb(err, results);
      });
    }
    else {
      cb(err, results);
    }
  });
};

ElasticSearchClient.prototype.delete = function (options, cb) {
  var self = this;
  var path = '/' + (options.index || this._index);
  path += '/' + options.type + '/' + options.id;
  this.exec({path: path, method: 'DELETE'}, function (err, results) {
    if (err) return cb(err);
    if (self._refresh.delete || options.refresh) {
      self.refreshIndex(options, function (err) {
        cb(err, results);
      });
    }
    else {
      cb(err, results);
    }
  });
};

ElasticSearchClient.prototype.createMapping = function (options, cb) {
  var path = '/' + (options.index || this._index);
  path += '/' + options.type;
  path += '/_mapping';
  this.exec({path: path, method: 'PUT', data: options.data}, cb);
};

ElasticSearchClient.prototype.deleteMapping = function (options, cb) {
  var path = '/' + (options.index || this._index);
  path += '/' + options.type + '/_mapping';
  this.exec({path: path, method: 'DELETE', data: options.data}, cb);
};

ElasticSearchClient.prototype.createIndex = function (options, cb) {
  if (arguments.length === 1) {
    cb = options;
    options = {};
  }
  this.exec({
    path: '/' + (options.index || this._index),
    method: 'PUT',
    data: options.data
  }, cb);
};

ElasticSearchClient.prototype.deleteIndex = function (options, cb) {
  if (arguments.length === 1) {
    cb = options;
    options = {};
  }
  var path = '/' + (options.index || this._index);
  this.exec({path: path, method: 'DELETE', data: options.data}, cb);
};

ElasticSearchClient.prototype.refreshIndex = function (options, cb) {
  if (arguments.length === 1) {
    cb = options;
    options = {};
  }
  var path = '/' + (options.index || this._index);
  path += '/_refresh';
  this.exec({path: path, method: 'POST', data: options.data}, cb);
};

ElasticSearchClient.prototype.exec = function (params, cb) {
  var self = this;
  var requestOptions = {
    uri: formatUrl({
      protocol: self._protocol,
      hostname: self._host,
      port: self._port,
      pathname: params.path || '', auth: self._auth
    }),
    method: params.method,
    headers: (params.data ? {
      'content-type': 'application/json',
      'content-length': params.data.length
    } : {}),
    json: params.data,
    qs: params.qs
  };
  if (typeof requestOptions.headers['content-length'] === 'undefined') {
    delete requestOptions.headers['content-length'];
  }

  //Retry settings
  var operation = retry.operation(self._retry);
  operation.attempt( function (attemptNum) {
    request(requestOptions, function (err, resp, body) {
      var details = {attempts: attemptNum, request: requestOptions};

      //Retry the operation on error
      if (operation.retry(err || (body ? body.error : null))) {
        return;
      }

      //When out of retries, return the error and number of attempts
      if (err) {
        return cb(operation.mainError(), details);
      }

      //Otherwise parse the body and finish
      if (body) {
        if (typeof body === 'string') {
          try {
            body = JSON.parse(body);
          }
          catch(err){
            return cb(err, details);
          }
        }
        if (resp.statusCode >=  400){
          var msg = (body && body.error ? body.error : "Elastic Search response: " + resp.statusCode)
          var error = new Error(msg);
          error.statusCode = resp.statusCode;
          return cb(error, details);
        }
        if (body.error) {
          return cb(new Error(body.error), details);
        }
        body.attempts = attemptNum;
        return cb(null, body);
      }
      else {
        return cb();
      }
    });
  });
};
