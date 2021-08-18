var async = require("async");
var moment = require('moment');

var exchange = "Binance";
var logger = require('../util/logger').logger({name:exchange});
var WebSocketClient = require('websocket').client;

var util = require('../util/util.js')
var sortAndMergeTrades = util.sortAndMergeTrades;
var cgui = require('../util/cgui.js')
var marketLog = cgui.marketLog;
var marketError = cgui.marketError;

// Global status
var pairsName = util.getCliPairs();
if (pairsName.length == 0)
	pairsName = ['BTC-ETH', 'BTC-LTC'];
var pairsMap = {};
for (var i in pairsName) {
	var p = pairsName[i].split('-');
	pairsMap[(p[1] + p[0]).toLowerCase()] = pairsName[i];
}
cgui.init({
	exchange:	exchange,
	pairsName:	pairsName,
	pairsMap:	pairsMap
});
var orderbookBids = {};
var orderbookAsks = {};
var marketHistory = {};
for(var p in pairsMap) {
	orderbookBids[pairsMap[p]] = [];
	orderbookAsks[pairsMap[p]] = [];
	marketHistory[pairsMap[p]] = [];
}
var maxMemory = 99;
var debug = false;

//////////////////////////////////////////////////////
// Data processing
//////////////////////////////////////////////////////
var lastValidDepthMsgTime = {};
var lastValidTradeMsgTime = {};

function handleDepthData(market, data) {
	var bids = [];
	for (var i in data['bids'])
		bids.push({'p':parseFloat(data['bids'][i][0]),'s':parseFloat(data['bids'][i][1])});
	var asks = [];
	for (var i in data['asks'])
		asks.push({'p':parseFloat(data['asks'][i][0]),'s':parseFloat(data['asks'][i][1])});
	orderbookBids[market] = bids;
	orderbookAsks[market] = asks;

	marketLog(market, "Full depth got:" + market + ' ' + bids.length + '/' + asks.length + ' ' + (new Date()));

	var result = cgui.markUpdate(market, {
		'orderbookBids'	:	orderbookBids[market],
		'orderbookAsks'	:	orderbookAsks[market]
	});

	// Trim data.
	orderbookBids[market] = orderbookBids[market].slice(0, maxMemory);
	orderbookAsks[market] = orderbookAsks[market].slice(0, maxMemory);
}

function handleDepthUpdateData(market, data) {
	var ts = data.E;
	if (orderbookBids[market] == null || orderbookAsks[market] == null)
		return;
	lastValidDepthMsgTime[market] = Date.now();
	var bids = orderbookBids[market];
	var asks = orderbookAsks[market];
	var bidsUpdate = data['b'];
	var asksUpdate = data['a'];
	for (var i in bidsUpdate) {
		var p = parseFloat(bidsUpdate[i][0]);
		var s = parseFloat(bidsUpdate[i][1]);
		var changed = false;
		do {
			changed = false;
			for (var j in bids) {
				j = parseInt(j);
				if (bids[j].p == p) {
					bids.splice(j, 1);
					changed = true;
					break;
				}
			}
		} while (changed);
		if (s > 0) bids.push({'p':p,'s':s});
	}
	for (var i in asksUpdate) {
		var p = parseFloat(asksUpdate[i][0]);
		var s = parseFloat(asksUpdate[i][1]);
		var changed = false;
		do {
			changed = false;
			for (var j in asks) {
				if (asks[j].p == p) {
					asks.splice(j, 1);
					changed = true;
					break;
				}
			}
		} while (changed);
		if (s > 0) asks.push({'p':p,'s':s});
	}
	var result = cgui.markUpdate(market, {
		'orderbookBids'	:	bids,
		'orderbookAsks'	:	asks,
		'marketTime' : ts
	});

	// Trim data.
	orderbookBids[market] = bids.slice(0, maxMemory);
	orderbookAsks[market] = asks.slice(0, maxMemory);
}

var lastTradeId = {};
function handleTradeData(market, data) {
	if (lastTradeId[market] == null)
		lastTradeId[market] = 0;
	if (lastTradeId[market] >= data['T']) return;
	lastValidTradeMsgTime[market] = Date.now();
	lastTradeId[market] = data['T'];
	var fills = [];
	fills.push({
		'T':(data['m'] ? 'SELL' : 'BUY'),
		'p':data['p'],
		's':data['q'],
		't':moment(data['T']).format("YYYY-MM-DDTHH:mm:ss.SSS")
	});
	fills = sortAndMergeTrades(
			exchange,
			fills
		);
	fills = fills.slice(0, maxMemory);
	var result = cgui.markUpdate(market, {
		'fills'		 	:	fills
	});
}

//////////////////////////////////////////////////////
// Requesting
//////////////////////////////////////////////////////

function requestDepth(symbol, callback) {
	var market = pairsMap[symbol];
	var options = {
		protocol:'https:',
		hostname: 'www.binance.com',
		path: '/api/v1/depth?limit=100&symbol='+symbol.toUpperCase(),
		headers: {
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			'Host': 'www.binance.com',
			'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:72.0) Gecko/20100101 Firefox/72.0'
		},
		timeout:10*1000,
		times:1,
		interval:0
	};
	marketLog(market, "Requesting depth:" + JSON.stringify(options));
	var url = 'https://www.binance.com/api/v1/depth?limit=100&symbol='+symbol.toUpperCase();
	async.waterfall([
		function(wtf_cb) {
			// util.httpget(options, wtf_cb);
			util.httpsGet(url, wtf_cb);
		}, function(res, wtf_cb) {
			marketLog(market, "Full depth:" + res.length + " "+ (new Date()));
			try {
				res = JSON.parse(res);
				if (res['code'] != null && res['code'] != 0)
					marketLog(market, res['msg']);
				else
					handleDepthData(market, res);
				return wtf_cb(null, res);
			} catch (err) {	return wtf_cb(err, res); }
		}], function(err, res){
			if (err != null)
				marketLog(market, "REST Error " + JSON.stringify(err) + " "+ (new Date()));
			callback();
		});
}
var symbols = [];
for (var symbol in pairsMap)
	symbols.push(symbol);
async.eachSeries(symbols, requestDepth);
// Add a monitor to check lastValidMsgTime once 120 seconds.
setInterval(function(){
	var max_past_trade_t = 0;
	var max_past_trade_m = null;
	for (var market in lastValidTradeMsgTime) {
		var past = Date.now() - lastValidTradeMsgTime[market];
		if (past > max_past_trade_t) {
			max_past_trade_t = past;
			max_past_trade_m = market;
		}
	}
	var max_past_depth_t = 0;
	var max_past_depth_m = null;
	for (var market in lastValidDepthMsgTime) {
		var past = Date.now() - lastValidDepthMsgTime[market];
		if (past > max_past_depth_t) {
			max_past_depth_t = past;
			max_past_depth_m = market;
		}
	}
	cgui.globalLog("oldest trade " + max_past_trade_m + ": " + max_past_trade_t/1000 + "s");
	cgui.globalLog("oldest depth " + max_past_depth_m + ": " + max_past_depth_t/1000 + "s");
	// Trade data could be much less than depth
	if (max_past_trade_m != null && max_past_trade_m != null)
		if (max_past_trade_t < 1200*1000 && max_past_depth_t < 120*1000)
			return;
	if (debug)
		logger.error('No updates ' + (new Date()));
	else
		cgui.globalLog('No updates ' + (new Date()));
	process.exit();
}, 10*1000);
for (var symbol in pairsMap) {
	var market = pairsMap[symbol];
	var client = new WebSocketClient();
	var url = 'wss://stream.binance.com:9443/ws/'+symbol+'@depth@100ms';
	client.connect(url);
	client.on('connect', function(connection) {
		logger.info('WebSocket depth[' + market + '] Connected');
		connection.on('error', function(error) {
			logger.error('Connection Error[' + market + ']: ' + error.toString());
		});
		connection.on('close', function() {
			logger.info('Connection Closed[' + market + ']');
			client.connect(url);
		});
		connection.on('message', function(message) {
			if (message.type === 'utf8')
				message = message.utf8Data;
			else return;
			message = JSON.parse(message);
			var actualMarket = pairsMap[message['s'].toLowerCase()];
			handleDepthUpdateData(actualMarket, message);
		});
	});
}
for (var symbol in pairsMap) {
	var market = pairsMap[symbol];
	var client = new WebSocketClient();
	var url = 'wss://stream.binance.com:9443/ws/'+symbol+'@aggTrade';
	client.connect(url);
	client.on('connect', function(connection) {
		logger.info('WebSocket trade[' + market + '] Connected');
		connection.on('error', function(error) {
			logger.error('Connection Error[' + market + ']: ' + error.toString());
		});
		connection.on('close', function() {
			logger.info('Connection Closed[' + market + ']');
			client.connect(url);
		});
		connection.on('message', function(message) {
			if (message.type === 'utf8')
				message = message.utf8Data;
			else return;
			message = JSON.parse(message);
			var actualMarket = pairsMap[message['s'].toLowerCase()];
			handleTradeData(actualMarket, message);
		});
	});
}
if (debug != true && pairsName.length < 10)
	cgui.start();
