var conf = module.exports;

function abort(msg) {
	console.error(msg);
	process.exit(-1);
}

conf.redisHost = process.env.REDIS_HOST || abort("No REDIS_HOST");
conf.redisPort = parseInt(process.env.REDIS_PORT || abort("No REDIS_PORT"));
conf.redisOptions = {'detect_buffers':true, 'auth_pass':(process.env.REDIS_PSWD || abort("No REDIS_PSWD"))};
