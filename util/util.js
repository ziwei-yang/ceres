var http = require("http");
var https = require("https");
var StringDecoder = require('string_decoder').StringDecoder;
var colors = require('colors');
var pad = require('pad');
var redis = require('../util/redis').client;
var async = require("async");
var moment = require('moment');
var exp = module.exports;

exp.getMonthLastFridayList = function(timeString) {
	// Default time: UTC 12:00
	if (timeString == null) timeString = 'T12:00:00.000+00:00';
	var now = moment();
	var currentDOM = now.date();
	var currentDOW = now.day();
	var currentMonth = now.month()+1;
	if (currentMonth < 10)
		currentMonth = '0'+currentMonth;
	else
		currentMonth = ''+currentMonth;
	// Get last Friday
	var fridays = [];
	var startTime = moment(""+now.year()+'-'+currentMonth+'-01'+timeString);
	for (var m = 0; m <= 11; m++) {
		var time = startTime.clone().add(m, 'M');
		var month = time.month();
		var lastFriday = null;
		while (time.add(1, 'd').month() == month) {
			if (time.day() == 5)
				lastFriday = time.clone();
		}
		fridays.push(lastFriday);
	}
	return fridays;
}

exp.getMonthCode = function(momentTime) {
	var codes = ['F','G','H','J','K','M','N','Q','U','V','X','Z'];
	return "" + codes[momentTime.month()] + '' + (momentTime.year()%100);
}

// Return next 2 Friday (including today)
exp.getActiveWeek = function(timeString) {
	// Default time: UTC 12:00
	if (timeString == null) timeString = 'T12:00:00.000+00:00';
	var now = moment();
	var currentDOW = now.day();
	var startTime = moment(now.format('YYYY-MM-DD')+timeString);
	if (currentDOW <= 5) {
		var nextFriday = startTime.clone().add(5-currentDOW, 'd');
		var nextNextFriday = nextFriday.clone().add(7, 'd');
		return [nextFriday, nextNextFriday];
	} else {
		var nextFriday = startTime.clone().add(12-currentDOW, 'd');
		var nextNextFriday = nextFriday.clone().add(7, 'd');
		return [nextFriday, nextNextFriday];
	}
}

exp.getActiveMonth = function() {
	var fridays = exp.getMonthLastFridayList();
	// If this month will end in 7 days, return this and next month.
	var now = moment();
	if ((fridays[0]- now)/1000/3600/24 < 7) {
		if ((fridays[0]- now)/1000/3600/24 > 0)
			return [fridays[0], fridays[1]];
		else
			return [fridays[1]];
	} else
		return [fridays[0]];
}

exp.getActiveQuater = function() {
	var fridays = exp.getMonthLastFridayList().filter(t => (t.month()+1)%3 == 0);
	// If this month will end in 7 days, return this and next month.
	var now = moment();
	if ((fridays[0]- now)/1000/3600/24 < 7) {
		if ((fridays[0]- now)/1000/3600/24 > 0)
			return [fridays[0], fridays[1]];
		else
			return [fridays[1]];
	} else
		return [fridays[0]];
}

exp.getCliPairs = function() {
	var args = process.argv.slice(2);
	var pairs = [];
	if (args.length == 0) return [];
	var base = 'BTC';
	if (args[0].toUpperCase() == 'USDT' && args.length > 1) {
		base = 'USDT';
		args = args.slice(1);
	} else if (args[0].toUpperCase() == 'USD' && args.length > 1) {
		base = 'USD';
		args = args.slice(1);
	}
	for (var i in args)
		if (args[i].split('-').length >= 2)
			pairs.push(args[i].toUpperCase());
		else
			pairs.push(base + '-' + args[i].toUpperCase());
	var uniqPairMap = {}; // Uniq pairs.
	pairs.forEach((p) => { uniqPairMap[p] = 1; });
	return Object.keys(uniqPairMap);
}

// From highest price to lowest price.
exp.sortBuyOrder = sortBuyOrder = function(exchange) {
	var key = 'p';
	var key2 = 's';
	return function (order1, order2) {
		if (order1[key] != order2[key])
			return order2[key] - order1[key];
		return order1[key2] - order2[key2];
	};
}
// From lowest price to highest price.
exp.sortSellOrder = sortSellOrder = function(exchange) {
	var key = 'p';
	var key2 = 's';
	return function (order1, order2) {
		if (order1[key] != order2[key])
			return order1[key] - order2[key];
		return order1[key2] - order2[key2];
	};
}
// From latest to oldest
// For buy trades at same timestamp: from low price to high price.
// For sell trades at same timestamp: from high price to low price.
exp.sortFilledOrder = function (exchange) {
	return function (order1, order2) {
		if (order1['i'] != null && order2['i'] != null)
			return order2['i']-order1['i'];
		var ret = parseTime(exchange, order2).localeCompare(parseTime(exchange, order1));
		if (ret != 0) return ret;
		// Compare by price if both timestamps are equal.
		if (order1['T'] == 'BUY' && order2['T'] == 'BUY')
			return parsePrice(exchange, order1) - parsePrice(exchange, order2);
		else if (order1['T'] == 'SELL' && order2['T'] == 'SELL')
			return parsePrice(exchange, order2) - parsePrice(exchange, order1);
		return 0;
	};
}

exp.sortAndMergeOrderbook = function (exchange, market, orderbook, option) {
	option = option || {};
	var type = option['type'] || 'bid';
	var omitOrderbookSize = option['omitOrderbookSize'];
	if (omitOrderbookSize == null) {
		if (market.indexOf('BTC-') == 0)
			omitOrderbookSize = 0.00005;
		else if (market.indexOf('USD-') == 0)
			omitOrderbookSize = 5;
		else if (market.indexOf('USDT-') == 0)
			omitOrderbookSize = 5;
		else if (market.indexOf('ETH-') == 0)
			omitOrderbookSize = 0.0002;
		else if (market.indexOf('EUR-') == 0)
			omitOrderbookSize = 10;
		else
			omitOrderbookSize = 0;
	}
	var newOrderBook = null;
	if (option.skip_sorting == true) {
		newOrderBook = orderbook; // Skip sorting.
	} if (type == 'bid') {
		newOrderBook = orderbook.sort(sortBuyOrder(exchange));
	} else if (type == 'ask') {
		newOrderBook = orderbook.sort(sortSellOrder(exchange));
	} else
		throw Error("Unexpected order type:" + type);
	if (omitOrderbookSize <= 0)
		return newOrderBook;

	// Filter orderbook by omitOrderbookSize.
	var odbk = newOrderBook.filter((o) => parseSize(exchange, o)*parsePrice(exchange, o) >= omitOrderbookSize);
	return odbk;
}

exp.binaryUpdateAsks = function(asks, p, s, maxLen=null) {
	var pos = exp.binarySearchInAsks(asks, p);
	var len = asks.length;
	if (pos == len) {
		if (s > 0 && (maxLen == null || len < maxLen))
			asks.push({ 'p':p, 's':s });
	} else if (asks[pos].p == p) {
		if (s > 0) asks[pos].s = s;
		else asks.splice(pos, 1);
	} else { // Insert only if need
		if (s > 0) asks.splice(pos, 0, { 'p':p, 's':s });
	}
	return asks;
}
exp.binaryUpdateBids = function(bids, p, s, maxLen=null) {
	var pos = exp.binarySearchInBids(bids, p);
	var len = bids.length;
	if (pos == len) {
		if (s > 0 && (maxLen == null || len < maxLen))
			bids.push({ 'p':p, 's':s });
	} else if (bids[pos].p == p) {
		if (s > 0) bids[pos].s = s;
		else bids.splice(pos, 1);
	} else { // Insert only if need
		if (s > 0) bids.splice(pos, 0, { 'p':p, 's':s });
	}
	return bids;
}
// Search price index in sorted asks low->high, find pos to replace or insert.
exp.binarySearchInAsks = function(asks, price) {
	var len = asks.length;
	if (len == 0) return 0;
	var fromPos = 0;
	var endPos = len-1;
	while(true) {
		var r = binarySearchOdbkRange(asks, price, fromPos, endPos, true);
		if (r[0] == true) return r[1];
		fromPos = r[1];
		endPos = r[2];
	}
}
exp.binarySearchInBids = function(asks, price) {
	var len = asks.length;
	if (len == 0) return 0;
	var fromPos = 0;
	var endPos = len-1;
	while(true) {
		var r = binarySearchOdbkRange(asks, price, fromPos, endPos, false);
		if (r[0] == true) return r[1];
		fromPos = r[1];
		endPos = r[2];
	}
}
// return [true, pos] if hit
// return [false, newRangeFrom, newRangeTo] if not
function binarySearchOdbkRange(array, price, fromPos, endPos, isAsk) {
	if (fromPos > endPos)
		throw Error("Unexpected range:" + fromPos + "," + endPos);
	else if (fromPos == endPos) {
		if (isAsk) {
			if (array[fromPos].p >= price) return [true, fromPos];
			return [true, fromPos+1];
		} else {
			if (array[fromPos].p <= price) return [true, fromPos];
			return [true, fromPos+1];
		}
	} else { // fromPos < endPos
		var midPos = parseInt((fromPos+endPos)/2);
		if (array[midPos].p == price)
			return [true, midPos];
		if (isAsk) {
			if (array[midPos].p < price) {
				return [false, midPos+1, endPos]; // search in new range
			} else { // mid price > price
				if (fromPos == midPos) // happens when fromPos+1 == endPos
					return [true, midPos];
				return [false, fromPos, midPos-1]; // search in new range
			}
		} else {
			if (array[midPos].p > price) {
				return [false, midPos+1, endPos]; // search in new range
			} else { // mid price < price
				if (fromPos == midPos) // happens when fromPos+1 == endPos
					return [true, midPos];
				return [false, fromPos, midPos-1]; // search in new range
			}
		}
	}
}

exp.sortAndMergeTrades = function (exchange, fills, option) {
	option = option || {};

	var bid1 = option.bid1;
	var ask1 = option.ask1;
	if (bid1 != null && ask1 != null) {
		// Tag trades with guessed direction.
		for (var i=0; i < fills.length; i++) {
			if (fills[i]['T'] != null) continue;
			var price = parsePrice(exchange, fills[i]);
			if (ask1 - price < price - bid1)
				fills[i]['T'] = 'BUY';
			else
				fills[i]['T'] = 'SELL';
		}
	}

	var compactFills = [];
	for (var i=0; i < fills.length; i++) {
		if (i == 0) {
			compactFills.push(fills[i]);
			continue;
		}
		var merged = false;
		if (parseTime(exchange, fills[i]) == parseTime(exchange, fills[i-1])) {
			if (parsePrice(exchange, fills[i]) == parsePrice(exchange, fills[i])) {
				merged = true;
				incOrderSize(exchange, compactFills[compactFills.length-1], parseSize(exchange, fills[i]));
			}
		}
		if (merged == false)
			compactFills.push(fills[i]);
	}
	return compactFills;
}

exp.round = function (num, decimal) {
	if (decimal == null) decimal = 3;
	var x = Math.pow(10, decimal);
	return Math.round(num * x)/x;
}
exp.formatNum = formatNum = function (num, decimal=6, fraction=10, suffix='') {
	if (num == null) return pad('', decimal+fraction+1);
	num = exp.round(num, fraction);
	var str = (num+'');
	if (num < 0.000001) str = Number(num).toFixed(fraction).replace(/0+$/g, '');
	if (suffix != null) str += suffix;
	var segs = str.split('.');
	var ld = segs[0].length;
	if (ld < decimal)
		segs[0] = pad(decimal, segs[0]);
	if (segs.length == 1)
		segs.push(pad(fraction, ''));
	else
		segs[1] = pad(segs[1], fraction);
	if (segs[1].trim().length == 0)
		return segs.join(' ');
	else
		return segs.join('.');
}

exp.parseSize = parseSize = function (exchange, order) {
	return order['s'];
}
exp.setOrderSize = setOrderSize = function (exchange, order, size) {
	return (order['s'] = size);
}
exp.incOrderSize = incOrderSize = function (exchange, order, size) {
	return (order['s'] = parseFloat(order['s']) + parseFloat(size));
}
exp.parseTime = parseTime = function (exchange, order) {
	return order['t'];
}
exp.parsePrice = parsePrice = function (exchange, order) {
	if (order == null) return null;
	return order['p'];
}

exp.stringifyOrder = function (exchange, order, type) {
	if (order == null || order == undefined) return pad('', 23);
	var key = 'p';
	var key2 = 's';
	var key3 = 'T'; // Filled trade type.

	if (type == 'FIL') {
		if (order[key] < 0.00001)
			if (order[key2] > 10000)
				return pad((order[key3] || '-').slice(0,3),1) + " " +
					formatNum(order[key], 1, 11) + "" +
					formatNum(parseSize(exchange, order)/1000, 10, 0, 'k') +
					(parseTime(exchange, order).split('T')[1]);
			else
				return pad((order[key3] || '-').slice(0,3),1) + " " +
					formatNum(order[key], 1, 11) + "" +
					formatNum(parseSize(exchange, order), 9, 0) + ' ' +
					(parseTime(exchange, order).split('T')[1]);
		else
			if (order[key2] > 10000)
				return pad((order[key3] || '-').slice(0,3),1) + " " +
					formatNum(order[key], 6, 10) + "" +
					formatNum(parseSize(exchange, order)/1000, 7, 0, 'k') + '      ' +
					(parseTime(exchange, order).split('T')[1]);
			else
				return pad((order[key3] || '-').slice(0,3),1) + " " +
					formatNum(order[key], 6, 10) + "" + formatNum(parseSize(exchange, order),6,6) + 
					" " + (parseTime(exchange, order).split('T')[1]);
	}
	// Print as compact ask/bid order.
	if (order[key] < 0.00001)
		if (order[key2] > 10000)
			return formatNum(order[key], 1, 11) + "" + formatNum(parseSize(exchange, order)/1000, 9, 0, 'k');
		else
			return formatNum(order[key], 1, 11) + "" + formatNum(parseSize(exchange, order), 8, 0) + ' ';
	else if (order[key] < 0.001)
		if (order[key2] > 10000)
			return formatNum(order[key], 1, 10) + "" + formatNum(parseSize(exchange, order)/1000, 10, 0, 'k');
		else
			return formatNum(order[key], 1, 10) + "" + formatNum(parseSize(exchange, order), 9, 0) + ' ';
	else if (order[key] < 10)
		if (order[key2] > 10000)
			return formatNum(order[key], 1, 10) + "" + formatNum(parseSize(exchange, order)/1000, 8, 0, 'k') + '  ';
		else
			return formatNum(order[key], 1, 10) + "" + formatNum(parseSize(exchange, order), 7, 3);
	else
		if (order[key2] > 2000)
			return formatNum(order[key], 6, 5) + "" + formatNum(parseSize(exchange, order)/1000, 8, 0, 'k') + '  ';
		else
			return formatNum(order[key], 6, 8) + "" + formatNum(parseSize(exchange, order), 4, 5);
}

exp.traderStatus = function(pairNameArray, callback) {
	var orders = {};
	var marketInfo = {};
	uniqPairNameArray = pairNameArray.filter(function(elem, pos) {
		    return pairNameArray.indexOf(elem) == pos;
	});
	async.eachSeries(
		uniqPairNameArray,
		function(pairName, es_cb) {
			async.waterfall([
				function(wtf_cb) {
					var key = "URANUS:orders:" + pairName;
					redis.hvals(key, wtf_cb)
				}, function(vals, wtf_cb) {
					var legacyOrders = [];
					var currentOrders = [];
					var childOrders = [];
					var low_balance_market = [];
					var balanceMap = {};
					var deviationMap = {};
					for (var i in vals) {
						var d = JSON.parse(vals[i]);
						var t = d['t']; // Timestamp
						low_balance_market = d['low_balance_market'];
						legacyOrders = legacyOrders.concat(d['legacy']);
						currentOrders = currentOrders.concat(d['current']);
						childOrders = childOrders.concat(d['child']);
						if (d['balance'] != null)
							for (var k in d['balance'])
								balanceMap[k] = d['balance'][k];
						if (d['deviation'] != null)
							for (var k in d['deviation'])
								deviationMap[k] = d['deviation'][k];
					}
					orders[pairName] = {
						'legacy':legacyOrders,
						'current':currentOrders,
						'child':childOrders
					};
					marketInfo[pairName] = {
						'balance':balanceMap,
						'deviation':deviationMap,
						'low_balance_market':low_balance_market
					};
					wtf_cb(null, true);
				}], es_cb);
		},
		function(err) {
			callback(err, orders, marketInfo);
		});
}

// mset then broadcast channel -> 1
exp.broadcast = function (channelPrefix, snapshot, options, callback) {
	var bids = snapshot['bids'];
	var asks = snapshot['asks'];
	var trades = snapshot['trades'];
	var info = snapshot['info'];
	options = options || {};
	// Update redis.
	var now = Date.now();
	var flushTime = now;
	var pubTime = null;
	var orderbook = [ bids, asks, now ];
	// trades has timestamp already, so dont need marketTime.
	if (options.marketTime) // marketTime in ms, might be null.
		orderbook.push(options.marketTime);
	async.waterfall([
		function(wtf_cb) {
			var setKV = [];
			if (options['infoUpdated'] == true && info != null)
				setKV = setKV.concat([channelPrefix+':info', JSON.stringify([info, now])]);
			if (options['odbkUpdated'] == true && orderbook != null)
				setKV = setKV.concat([channelPrefix+':orderbook', JSON.stringify(orderbook)]);
			if (options['tickUpdated'] == true && trades != null)
				setKV = setKV.concat([channelPrefix+':trades', JSON.stringify([trades, now])]);
			var args = setKV.concat([wtf_cb]);
			if (setKV.length == 1)
				return callback();
			else if (setKV.length == 3)
				redis.set.apply(redis, args);
			else
				redis.mset.apply(redis, args);
		}, function(res, wtf_cb) {
			pubTime = Date.now();
			flushTime = pubTime - flushTime;
			// Publish notifications async, after data is set.
			if (options['odbkUpdated'] == true)
				redis.publish(channelPrefix + ':orderbook_channel', 1);
			if (options['tickUpdated'] == true)
				redis.publish(channelPrefix + ':trades_channel', 1);
			wtf_cb(null, 'OK');
		}], function(err, res) {
			pubTime = Date.now() - pubTime;
			callback(err, {
				'pubTime': pubTime,
				'flushTime': flushTime
			});
		});
}

// broadcast channel -> data
exp.broadcastNew = function (channelPrefix, snapshot, options, callback) {
	var bids = snapshot['bids'];
	var asks = snapshot['asks'];
	var newFills = snapshot['newFills'] || []; // Only broadcast new fills
	var info = snapshot['info'];
	options = options || {};
	// Update redis.
	var now = Date.now();
	var pubTime = Date.now();
	var orderbook = [ bids, asks, now ];
	// trades has timestamp already, so dont need marketTime.
	if (options.marketTime) // marketTime in ms, might be null.
		orderbook.push(options.marketTime);
	async.waterfall([
		function(wtf_cb) {
			if (options['infoUpdated'] == true && info != null)
				redis.publish(
					channelPrefix+':full_info_channel',
					JSON.stringify([info, now]),
					wtf_cb
				);
			else
				wtf_cb(null, null);
		}, function(res, wtf_cb) {
			if (options['odbkUpdated'] == true && orderbook != null)
				redis.publish(
					channelPrefix+':full_odbk_channel',
					JSON.stringify(orderbook),
					wtf_cb
				);
			else
				wtf_cb(null, null);
		}, function(res, wtf_cb) {
			if (options['tickUpdated'] == true && newFills != null)
				redis.publish(
					channelPrefix+':full_tick_channel',
					JSON.stringify([newFills, now]),
					wtf_cb
				);
			else
				wtf_cb(null, null);
		}], function(err, res) {
			pubTime = Date.now() - pubTime;
			callback(err, {
				'pubTime': pubTime
			});
		});
}

// Support http/http with retry options.
// Set options['method'] to 'POST'/'PUT'/... Default: GET
// https://nodejs.org/api/http.html
exp.httpRequest = exp.httpget = httpget = function(options, callback) {
	var httpLib = null;
	if (options['protocol'] == 'http:')
		httpLib = http;
	else if (options['protocol'] == 'https:')
		httpLib = https;
	else
		return callback('unknown protocol ' + options['protocol']);
	async.retry(
		{
			times:options['times'] || 5,
			interval:options['interval'] || 0
		}, function (rty_cb) {
			var finished = false;
			httpLib.request(options, function(http_response) {
				console.log("http response open");
				var str = '';
				var decoder = new StringDecoder('utf8');
				http_response.on('data', function (chunk) {
					str += decoder.write(chunk);
				});
				http_response.on('end', function () {
					if (finished == true) return;
					finished = true;
					rty_cb(null, str);
				});
			}).on('error', function(error){
				console.log("http error", error);
				if (finished == true) return;
				finished = true;
				rty_cb(error.message, null);
			});
		}, callback);
}

exp.httpsGet = function(url, callback) {
	var finished = false;
	https.get(url, (res) => {
		var str = '';
		var decoder = new StringDecoder('utf8');
		res.on('data', (d) => {
			str += decoder.write(d);
		});

		res.on('end', (d) => {
			if (finished == true) return;
			finished = true;
			callback(null, str);
		});

	}).on('error', (e) => {
		console.error(e);
		callback(e, null);
	});
}
