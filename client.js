var formatUrl = require('url').format
  , querystring = require('querystring')
  , request = require('request')
  , retry = require('retry')
  , Memcached = require('memcached');

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

  if (options.memcached) {
    self._memcachedHost = options.memcachedHost || self._host;
    self._memcachedPort = options.memcachedPort || 11211;
    self._memcached = new Memcached(self._memcachedHost + ':' + self._memcachedPort);
  }
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
  if (this._memcached) {
    this.execMemcached(params, cb);
  }
  else {
    this.execREST(params, cb);
  }
};

ElasticSearchClient.prototype.execREST = function (params, cb) {
  var self = this;
  var requestOptions = {
    uri: formatUrl({
      protocol: self._protocol,
      hostname: self._host,
      port: self._port,
      pathname: params.path || '',
      auth: self._auth
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
          var msg = (body && body.error ? body.error : "Elastic Search response: " + resp.statusCode);
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

ElasticSearchClient.prototype.execMemcached = function (params, cb) {
  if (params.method === 'GET') {
    var query = params.qs || {};
    if (params.data) {
      query.source = params.data;
    }
    var key = formatUrl({
      pathname: params.path || '',
      auth: this._auth,
      query: query
    });
    this._memcached.get(key, function (err, result) {
      if (err) return cb(err);
      try {
        result = JSON.parse(result);
        if (!result.exists) {
          err = new Error('Does not exist');
          err.statusCode = 404;
          return cb(err);
        }
        cb(null, result);
      }
      catch (e) {
        cb(e);
      }
    });
  }
  else {
    this.execREST(params, cb);
  }
};
