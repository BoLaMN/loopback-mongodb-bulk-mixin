'use strict'

bulk = require './bulk'

module.exports = (app) ->
  app.loopback.modelBuilder.mixins.define 'Bulk', bulk

  return
