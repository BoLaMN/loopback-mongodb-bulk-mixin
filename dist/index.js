'use strict';
var bulk;

bulk = require('./bulk');

module.exports = function(app) {
  app.loopback.modelBuilder.mixins.define('Bulk', bulk);
};
