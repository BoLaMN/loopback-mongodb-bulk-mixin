Bulk = require './operations'

debug = require('debug')('loopback:mixins:bulk')

module.exports = (Model) ->

  Model.bulk = (data = {}, ordered, options, cb) ->
    if typeof data is 'function'
      return @bulk {}, false, {}, data

    if typeof ordered is 'function'
      return @bulk {}, false, {}, ordered
      
    if typeof options is 'function'
      return @bulk {}, ordered, {}, options

    connector = @getConnector()

    col = connector.collectionName @modelName

    bulk = new Bulk col, @, ordered, options
    keys = Object.keys data

    add = (key) ->
      ({ filter, data, options }) ->

        fn = (func) ->
          func[key] data, options or {}

        if filter 
          fn bulk.find filter

        fn bulk

    keys.forEach (key) ->
      data[key].forEach add key

    if cb 
      return bulk.execute options, cb 

    bulk 

  Model.remoteMethod 'bulk',
    accepts: [
      {
        arg: "data"
        description: "An object of model property name/value pairs"
        http:
          source: "body"
        type: "object"
      }
      {
        arg: "ordered"
        description: "Filter defining fields, where, aggregate, order, offset, and limit"
        type: "booleaan"
      }
      {
        arg: "options"
        description: "options"
        type: "object"
      }
    ]
    accessType: "READ"
    description: "Find all instances of the model matched by filter from the data source."
    http:
      path: "/bulk"
      verb: "post"
    returns:
      arg: "data"
      root: true
      type: 'object'

  return