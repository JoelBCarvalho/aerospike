const Aerospike = require('aerospike');
const asConfig = require('./aerospike_config');
var aerospikeConfig = asConfig.aerospikeConfig();
var aerospikeDBParams = asConfig.aerospikeDBParams();
const client = Aerospike.client(asConfig)

// Establish connection to the cluster
exports.connect = function (callback) {
client.connect(callback)
}
// Write a record
exports.writeRecord = function (k, v, callback) {
let key = new Aerospike.Key(aerospikeDBParams.defaultNamespace, aerospikeDBParams.defaultSet, k)
client.put(key, { greet: v }, function (error) {
 // Check for errors
 if (error) {
   // An error occurred
   return callback(error)
 } else {
   return callback(null, 'ok')
 }
})
}
// Read a record
exports.readRecord = function (k, callback) {
let key = new Aerospike.Key(aerospikeDBParams.defaultNamespace, aerospikeDBParams.defaultSet, k)
client.get(key, function (error, record) {
 // Check for errors
 if (error) {
   // An error occurred
   return callback(error)
 } else {
   let bins = record.bins
   return callback(null, k + ' ' + bins.greet)
 }
})
}

// Exists a record
exports.existsRecord = function (k, callback) {
let key = new Aerospike.Key(aerospikeDBParams.defaultNamespace, aerospikeDBParams.defaultSet, k)
client.exists(key, function (error, record) {
 // Check for errors
 if (error) {
   // handle error
 } else if (record) {
   // records exists
   return callback(null, true);
 } else {
   // record does not exist
 }
})
}
