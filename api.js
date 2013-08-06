var modeler = require('modeler')
  , ElasticSearchClient = require('./client')
  , hydration = require('hydration');

module.exports = function (_opts) {
  if (!_opts.client && !_opts.index) {
    throw new Error('You must specify a client or an index for a new client.');
  }

  var api = modeler(_opts);

  api.client = api.options.client || (new ElasticSearchClient(api.options));
  api.sort = api.options.sort || '_timestamp';

  function continuable (offset, limit, dir, cb) {
    var sort = {};
    sort[api.sort] = dir;
    (function next () {
      api.client.search({
        type: api.options.name,
        data: {
          from: offset,
          size: limit,
          fields: ['_id', api.sort],
          sort: [sort]
        }
      }, function (err, results) {
        if (err) return cb(err);
        if (!results.hits || !results.hits.hits.length) return cb(null, []);
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
    api.client.index({
      type: api.options.name,
      id: entity.id,
      data: hydration.dehydrate(entity)
    }, function (err, result) {
      cb(err);
    });
  };

  api._load = function (id, cb) {
    api.client.get({type: api.options.name, id: id}, function (err, result) {
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
    api.client.delete({type: api.options.name, id: id}, cb);
  };

  return api;
};
