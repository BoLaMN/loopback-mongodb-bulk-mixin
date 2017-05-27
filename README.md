# loopback-mongo-bulk-mixin

Bulk Operations For MongoDB Loopback Connector
 - mixin to enable bulk operations
 - ties in with observers

* npm install loopback-mongo-bulk-mixin --save

rest example

```
data = 
  delete: [ 
    { filter: {} }
    { filter: {}, multi: true }
  ]
  update: [
    { filter: {}, data: {}, upsert: false }
    { filter: {}, data: {}, upsert: false, multi: true }
  ]
  replace: [
    { filter: {}, data: {}, upsert: false }
  ]
  insert: [
    {}
  ]

ordered = false 

options = {}

modelName.bulk data, ordered, options, (err, data) ->


/api/:modelName/bulk 
```

script example 
```
bulk = MyModel.bulk()

bulk
  .find type: 'water'
  .update $set: level: 1,
    multi: true
  
bulk
 .find type: 'water'
 .update $inc: level: 2,
   multi: true

bulk.insert
  name: 'Spearow'
  type: 'flying'
  
bulk.insert
  name: 'Pidgeotto'
  type: 'flying'
  
bulk.insert
  name: 'Charmeleon'
  type: 'fire'
  
bulk
  .find type: 'flying'
  .remove()

bulk
  .find type: 'fire'
  .remove multi: true 

bulk
  .find type: 'water'
  .update $set: hp: 100

bulk.execute (err, res) ->
  console.log 'Done!'
```

License: MIT