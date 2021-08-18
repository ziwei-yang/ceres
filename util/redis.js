var redis = require("redis");
var conf = require('../conf/config.js');
var log = console.log;

var exp = module.exports;

// Provide one function to delete all field/value in redis hash.
function delHashAsync(hashName) {
	var rds = this;
	rds.hkeys(hashName, function(err, res) {
		if (err) return log('error in redis.hkeys', hashName, err);
		log(hashName, 'purge fields:', res.length);
		if (res.length == 0) return;
		hdelArgs = [hashName].concat(res);
		rds.hdel.apply(rds, hdelArgs, function(err, res) {
			if (err) return log('error in redis.hdel', hdelArgs, err);
			log(hashName, 'all fields purged');
		});
	});
}

exp.newClient = function () {
	log("New redis client");
	var client = redis.createClient(conf.redisPort, conf.redisHost, conf.redisOptions);
	client.on('connect'     , ()=>log('redis connect'));
	client.on('ready'       , ()=>log('redis ready'));
	client.on('reconnecting', ()=>log('redis reconnecting'));
	client.on('end'         , ()=>log('redis end'));
	client.on('error'       , log);
	client.delHashAsync = delHashAsync;
	return client;
};
exp.client = exp.newClient(); // Compatiablity for old version.
exp.client.delHashAsync = delHashAsync;
