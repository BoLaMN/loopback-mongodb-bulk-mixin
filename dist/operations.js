var Bulk, Promise, async, debug, inspect, keys, loopback;

debug = require('debug')('loopback:connector:mongodb:bulk');

Promise = require('bluebird');

async = require('async');

loopback = require('loopback');

inspect = require('util').inspect;

keys = {
  insert: 'documents',
  update: 'updates',
  "delete": 'deletes'
};

Bulk = (function() {
  function Bulk(colName, model1, ordered, options1) {
    this.colName = colName;
    this.model = model1;
    this.ordered = ordered != null ? ordered : false;
    this.options = options1 != null ? options1 : {};
    this.cmds = [];
    this.db = this.model.getConnector().db;
    this.curr = null;
    this.find = this._find.bind(this);
  }

  Bulk.prototype.toObject = function() {
    var obj;
    if (this.curr) {
      this.cmds.push(this.curr);
    }
    obj = {
      insert: 0,
      update: 0,
      remove: 0,
      batches: this.cmds.length
    };
    this.cmds.forEach(function(cmd) {
      var key;
      key = Object.keys(cmd)[0];
      return obj[key] += cmd[keys[key]].length;
    });
    return obj;
  };

  Bulk.prototype.toString = function() {
    return JSON.stringify(this.toObject());
  };

  Bulk.prototype._find = function(query) {
    var connector, modelName;
    connector = this.model.getConnector();
    modelName = this.model.modelName;
    query = connector.buildWhere(modelName, query);
    return {
      remove: (function(_this) {
        return function(options) {
          var limit, model, multi;
          if (options == null) {
            options = {};
          }
          multi = options.multi, model = options.model;
          if (!_this.curr) {
            _this.curr = {
              "delete": model || _this.colName,
              deletes: [],
              ordered: _this.ordered,
              writeConcern: {
                w: 1
              }
            };
          }
          if (!_this.curr["delete"]) {
            _this.cmds.push(_this.curr);
            _this.curr = {
              "delete": model || _this.colName,
              deletes: [],
              ordered: _this.ordered,
              writeConcern: {
                w: 1
              }
            };
          }
          limit = 1;
          if (multi) {
            limit = 0;
          }
          _this.curr.deletes.push({
            q: query,
            limit: limit
          });
        };
      })(this),
      update: (function(_this) {
        return function(data, options) {
          var model, multi, upsert;
          if (options == null) {
            options = {};
          }
          multi = options.multi, upsert = options.upsert, model = options.model;
          if (!_this.curr) {
            _this.curr = {
              update: model || _this.colName,
              updates: [],
              ordered: _this.ordered,
              writeConcern: {
                w: 1
              }
            };
          }
          if (!_this.curr.update) {
            _this.cmds.push(_this.curr);
            _this.curr = {
              update: model || _this.colName,
              updates: [],
              ordered: _this.ordered,
              writeConcern: {
                w: 1
              }
            };
          }
          _this.curr.updates.push({
            q: query,
            u: data,
            multi: multi || false,
            upsert: upsert || false
          });
        };
      })(this),
      replace: function(upd) {
        return this.update(upd, {
          multi: false
        });
      }
    };
  };

  Bulk.prototype.insert = function(data, options) {
    if (options == null) {
      options = {};
    }
    if (!this.curr) {
      this.curr = {
        insert: options.model || this.colName,
        documents: [],
        ordered: this.ordered,
        writeConcern: {
          w: 1
        }
      };
    }
    if (!this.curr.insert) {
      this.cmds.push(this.curr);
      this.curr = {
        insert: options.model || this.colName,
        documents: [],
        ordered: this.ordered,
        writeConcern: {
          w: 1
        }
      };
    }
    this.curr.documents.push(data);
  };

  Bulk.prototype.normalizeId = function(model, data) {
    var idName, idValue;
    idName = model.definition._ids[0].name;
    idValue = data._id;
    if (idValue === null || idValue === void 0) {
      delete data._id;
    } else {
      data[idName] = idValue;
    }
    if (idName !== '_id') {
      delete data._id;
    }
    return data;
  };

  Bulk.prototype.rewriteId = function(model, data) {
    var idName, idValue;
    data = (typeof inst.toObject === "function" ? inst.toObject(false) : void 0) || inst;
    idName = model.definition._ids[0].name;
    idValue = data[idName];
    if (idValue === null || idValue === void 0) {
      delete data[idName];
    } else {
      data._id = idValue;
    }
    if (idName !== '_id') {
      delete data[idName];
    }
    return data;
  };

  Bulk.prototype.execute = function(options, callback) {
    var broadcast, connector, db, hookStates, normalizeId, removeErrored, result, rewriteId;
    if (options == null) {
      options = {};
    }
    if (callback == null) {
      callback = function() {};
    }
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
    hookStates = {};
    db = this.db;
    rewriteId = this.rewriteId;
    normalizeId = this.normalizeId;
    connector = this.model.getConnector();
    result = {
      inserted: [],
      matched: 0,
      modified: 0,
      removed: 0,
      upserted: 0
    };
    removeErrored = function(cmd, key, indexes) {
      var arr, hooks, i, index, item;
      if (!(indexes != null ? indexes.length : void 0)) {
        return;
      }
      item = keys[key];
      arr = cmd[item];
      hooks = hookStates[item];
      indexes.sort(function(a, b) {
        return a - b;
      });
      i = 0;
      while (i < indexes.length) {
        index = indexes[i] - i;
        arr.splice(index, 1);
        hooks.splice(index, 1);
        i++;
      }
    };
    broadcast = (function(_this) {
      return function(phase, cmd, cb) {
        var finish, inc, insert, item, key, notify, remove, update;
        key = Object.keys(cmd)[0];
        item = keys[key];
        if (hookStates[item] == null) {
          hookStates[item] = [];
        }
        notify = function(model, type, context) {
          return new Promise(function(resolve, reject) {
            return model.notifyObserversOf(phase + ' ' + type, context, function(err, ctx) {
              if (err) {
                return reject(err);
              }
              return resolve(ctx);
            });
          });
        };
        inc = function(arg) {
          var Model, inst, instance;
          Model = arg.Model, instance = arg.instance;
          if (phase === 'before') {
            return;
          }
          if (key === 'insert') {
            inst = new Model((typeof instance.toObject === "function" ? instance.toObject(false) : void 0) || instance);
            inst.setId(instance.id);
            return result.inserted.push(inst);
          } else {
            return result[key]++;
          }
        };
        finish = function(res) {
          cmd[item] = res;
          return cmd;
        };
        insert = function() {
          var model;
          model = loopback.getModel(cmd.insert);
          return Promise.map(cmd[item], function(data, index) {
            var base, hookState;
            if (phase === 'after') {
              normalizeId(model, data);
            }
            hookState = (base = hookStates[item])[index] != null ? base[index] : base[index] = {};
            return notify(model, 'save', {
              Model: model,
              instance: data,
              isNewInstance: true,
              hookState: hookState,
              options: options
            }).tap(inc).then(function(ctx) {
              if (phase === 'before') {
                return rewriteId(model, ctx.instance);
              }
            });
          });
        };
        update = function() {
          var model;
          model = loopback.getModel(cmd.update);
          return Promise.map(cmd[item], function(obj, index) {
            var base, hookState;
            hookState = (base = hookStates[item])[index] != null ? base[index] : base[index] = {};
            return notify(model, 'save', {
              Model: model,
              where: obj.q,
              data: obj.u,
              hookState: hookState,
              options: options
            }).tap(inc).then(function(ctx) {
              var modelName;
              modelName = ctx.Model.modelName;
              obj.u = connector.parseUpdateData(modelName, ctx.data);
              return obj;
            });
          });
        };
        remove = function() {
          var model;
          model = loopback.getModel(cmd["delete"]);
          return Promise.map(cmd[item], function(obj, index) {
            var base, hookState;
            hookState = (base = hookStates[item])[index] != null ? base[index] : base[index] = {};
            return notify(model, 'delete', {
              Model: model,
              where: obj.q,
              hookState: hookState,
              options: options
            }).tap(inc).then(function(ctx) {
              obj.q = ctx.where;
              return obj;
            });
          });
        };
        return Promise.all((function() {
          switch (key) {
            case 'insert':
              return insert();
            case 'update':
              return update();
            case 'delete':
              return remove();
          }
        })()).then(finish).asCallback(cb);
      };
    })(this);
    if (this.curr) {
      this.cmds.push(this.curr);
    }
    return async.each(this.cmds, (function(_this) {
      return function(cmd, done) {
        return async.series([
          function(cb) {
            debug('before', inspect(cmd, false, null));
            return broadcast('before', cmd, cb);
          }, function(cb) {
            debug('command', inspect(cmd, false, null));
            return db.command(cmd, function(err, res) {
              var key, ref;
              if ((ref = res.writeErrors) != null ? ref.length : void 0) {
                key = Object.keys(cmd)[0];
                removeErrored(cmd, key, res.writeErrors.map(function(error) {
                  return error.index;
                }));
                if (result.errors == null) {
                  result.errors = {};
                }
                result.errors[key] = res.writeErrors;
              }
              debug('command after', err, res);
              return cb(err, res);
            });
          }, function(cb) {
            debug('after', inspect(cmd, false, null));
            return broadcast('after', cmd, cb);
          }
        ], done);
      };
    })(this), (function(_this) {
      return function(err) {
        if (err) {
          callback(err);
        }
        _this.cmds = [];
        result.ok = 1;
        return callback(null, result);
      };
    })(this));
  };

  return Bulk;

})();

module.exports = Bulk;