var modeler = require('modeler')
  , hydration = require('hydration');

module.exports = function (_opts) {
  if (!_opts.client) {
    throw new Error('You must provide an elasticsearch client.');
  }

  var api = modeler(_opts);

  api.client = api.options.client;
  api.sort = api.options.sort || '_timestamp';

  // Refresh the index after all saves, really should only be used in tests.
  api.refresh = api.options.refresh || false;

  function continuable (offset, limit, dir, cb) {
    var sort = {};
    sort[api.sort] = dir;
    (function next () {
      api.client.search({_type: api.options.name}, {
        from: offset,
        size: limit,
        fields: ['_id', api.sort],
        sort: [sort]
      }, function (err, results) {
        if (err) return cb(err);
        if (!results.hits || !results.hits.hits.length) return cb(null, [], next);
        cb(null, results.hits.hits.map(function (hit) {
          return hit._id;
        }), next);
      });
    })();
  }

  api._head = function (offset, limit, cb) {
    continuable(offset, limit, 'asc', cb);
  };

  api._tail = function (offset, limit, cb) {
    continuable(offset, limit, 'desc', cb);
  };

  api._save = function (entity, cb) {
    var doc = hydration.dehydrate(entity);
    api.client.index({_type: api.options.name, _id: ('' + entity.id)}, doc, function (err, result) {
      if (err) return cb(err);
      api._finish(cb);
    });
  };

  api._load = function (id, cb) {
    api.client.get({_type: api.options.name, _id: ('' + id)}, function (err, result) {
      if (err) {
        if (err.statusCode && err.statusCode === 404) {
          return cb(null, null);
        }
        return cb(err);
      }
      cb(null, hydration.hydrate(result._source));
    });
  };

  api._destroy = function (id, cb) {
    api.client.delete({_type: api.options.name, _id: ('' + id)}, function (err) {
      if (err) return cb(err);
      api._finish(cb);
    });
  };

  api._finish = function (cb) {
    if (api.refresh) {
      api.client.indices.refresh(function (e) {
        cb(e);
      });
    }
    else {
      cb();
    }
  };

  return api;
};
