var Bulk, debug;

Bulk = require('./operations');

debug = require('debug')('loopback:mixins:bulk');

module.exports = function(Model) {
  Model.bulk = function(data, ordered, options, cb) {
    var add, bulk, col, connector, keys;
    if (data == null) {
      data = {};
    }
    if (typeof data === 'function') {
      return this.bulk({}, false, {}, data);
    }
    if (typeof ordered === 'function') {
      return this.bulk({}, false, {}, ordered);
    }
    if (typeof options === 'function') {
      return this.bulk({}, ordered, {}, options);
    }
    connector = this.getConnector();
    col = connector.collectionName(this.modelName);
    bulk = new Bulk(col, this, ordered, options);
    keys = Object.keys(data);
    add = function(key) {
      return function(arg) {
        var data, filter, fn, options;
        filter = arg.filter, data = arg.data, options = arg.options;
        fn = function(func) {
          return func[key](data, options || {});
        };
        if (filter) {
          fn(bulk.find(filter));
        }
        return fn(bulk);
      };
    };
    keys.forEach(function(key) {
      return data[key].forEach(add(key));
    });
    if (cb) {
      return bulk.execute(options, cb);
    }
    return bulk;
  };
  Model.remoteMethod('bulk', {
    accepts: [
      {
        arg: "data",
        description: "An object of model property name/value pairs",
        http: {
          source: "body"
        },
        type: "object"
      }, {
        arg: "ordered",
        description: "Filter defining fields, where, aggregate, order, offset, and limit",
        type: "booleaan"
      }, {
        arg: "options",
        description: "options",
        type: "object"
      }
    ],
    accessType: "READ",
    description: "Find all instances of the model matched by filter from the data source.",
    http: {
      path: "/bulk",
      verb: "post"
    },
    returns: {
      arg: "data",
      root: true,
      type: 'object'
    }
  });
};
