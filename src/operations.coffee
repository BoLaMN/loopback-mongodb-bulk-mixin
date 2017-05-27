debug = require('debug')('loopback:connector:mongodb:bulk')

Promise = require 'bluebird'
async = require 'async'
loopback = require 'loopback'

{ inspect } = require 'util'

keys =
  insert: 'documents'
  update: 'updates'
  delete: 'deletes'

class Bulk
  constructor: (@colName, @model, @ordered = false, @options = {}) ->
    @cmds = []
    
    { @db } = @model.getConnector()
 
    @curr = null
    @find = @_find.bind @

  toObject: ->
    if @curr
      @cmds.push @curr

    obj = 
      insert: 0
      update: 0
      remove: 0
      batches: @cmds.length

    @cmds.forEach (cmd) ->
      key = Object.keys(cmd)[0]
      obj[key] += cmd[keys[key]].length

    obj

  toString: ->
    JSON.stringify @toObject()

  _find: (query) ->

    { buildWhere, parseUpdateData } = @model.getConnector()

    query = buildWhere query 

    remove: ({ multi, model }) =>
      if not @curr
        @curr =
          delete: model or @colName
          deletes: []
          ordered: @ordered
          writeConcern: w: 1

      if not @curr.delete
        @cmds.push @curr

        @curr =
          delete: model or @colName
          deletes: []
          ordered: @ordered
          writeConcern: w: 1
      
      limit = 1

      if multi 
        limit = 0 

      @curr.deletes.push
        q: query
        limit: limit

      return

    update: (upd, { multi, upsert, model }) =>
      data = parseUpdateData upd

      if not @curr
        @curr =
          update: model or @colName
          updates: []
          ordered: @ordered
          writeConcern: w: 1

      if not @curr.update
        @cmds.push @curr

        @curr =
          update: model or @colName
          updates: []
          ordered: @ordered
          writeConcern: w: 1

      @curr.updates.push
        q: query
        u: data
        multi: multi or false
        upsert: upsert or false

      return

    replace: (upd) ->
      @update upd, multi: false

  insert: (data, options = {}) ->
    if not @curr
      @curr =
        insert: options.model or @colName
        documents: []
        ordered: @ordered
        writeConcern: w: 1

    if not @curr.insert
      @cmds.push @curr

      @curr =
        insert: options.model or @colName
        documents: []
        ordered: @ordered
        writeConcern: w: 1

    @curr.documents.push data

    return

  rewriteId: (model, inst) ->
    data = inst.toObject?() or inst 

    idName = model.definition._ids[0].name
    idValue = data[idName]

    if idValue is null or idValue is undefined
      delete data[idName]
    else
      data._id = idValue

    if idName isnt '_id'
      delete data[idName]

    data

  execute: (options = {}, callback = ->) ->
    hookState = {}

    db = @db 
    rewriteId = @rewriteId

    result = 
      inserted: []
      matched: 0
      modified: 0
      removed: 0
      upserted: 0

    removeErrored = (arr, indexes) ->
      if not indexes?.length 
        return 

      indexes.sort (a, b) ->
        a - b
        
      i = 0
      
      while i < indexes.length
        index = indexes[i] - i
        arr.splice index, 1
        
        i++
      
      return

    broadcast = (phase, cmd, cb) =>  
      key = Object.keys(cmd)[0]
      item = keys[key] 

      notify = (model, type, context) ->
        new Promise (resolve, reject) ->
          model.notifyObserversOf phase + ' ' + type, context, (err, ctx) ->
            if err 
              return reject err 
            resolve ctx 

      inc = ({ Model, instance }) ->
        if phase is 'before'
          return 

        if key is 'insert'
          inst = new Model instance.toObject?() or instance
          inst.setId instance.id

          result.inserted.push inst
        else
          result[key]++

      finish = (res) ->
        cmd[item] = res 
        cmd 

      insert = ->
        model = loopback.getModel cmd.insert 

        Promise.map cmd[item], (data) ->
          notify model, 'save',
            Model: model
            instance: data
            isNewInstance: true
            hookState: hookState
            options: options
          .tap inc
          .then (ctx) ->
            rewriteId model, ctx.instance

      update = ->
        model = loopback.getModel cmd.update 

        Promise.map cmd[item], (obj) ->
          notify model, 'save',
            Model: model
            where: obj.q
            data: obj.u
            hookState: hookState
            options: options
          .tap inc
          .then (ctx) ->
            obj.u = ctx.data 
            obj

      remove = ->
        model = loopback.getModel cmd.delete 

        Promise.map cmd[item], (obj) ->
          notify model, 'delete',
            Model: model
            where: obj.q
            data: obj.u
            hookState: hookState
            options: options
          .tap inc
          .then (ctx) ->
            obj.u = ctx.data 
            obj 

      Promise.all switch key 
        when 'insert' then insert()
        when 'update' then update()
        when 'delete' then remove()
      .then finish
      .asCallback cb 

    if @curr
      @cmds.push @curr

    async.each @cmds, (cmd, done) ->
      
      async.series [
        (cb) -> 
          debug 'before', inspect cmd, false, null
          broadcast 'before', cmd, cb
        (cb) ->
          debug 'command', inspect cmd, false, null
          db.command cmd, (err, res) ->
            if res.writeErrors?.length
              key = Object.keys(cmd)[0]
              removeErrored cmd[keys[key]], res.writeErrors.map (error) ->
                error.index
              result.errors ?= {}
              result.errors[key] = res.writeErrors
            debug 'command after', err, res 
            cb err, res
        (cb) ->
          debug 'after', inspect cmd, false, null
          broadcast 'after', cmd, cb 
      ], done

    , (err) ->
      if err 
        callback err 

      result.ok = 1

      callback null, result

module.exports = Bulk