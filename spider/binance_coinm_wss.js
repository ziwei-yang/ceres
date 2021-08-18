var async = require("async");
var moment = require('moment');

var exchange = "BNCM";
var logger = require('../util/logger').logger({name:exchange});
var WebSocketClient = require('websocket').client;

var util = require('../util/util.js')
var sortAndMergeTrades = util.sortAndMergeTrades;
var cgui = require('../util/cgui.js')
var marketLog = cgui.marketLog;
var marketError = cgui.marketError;

// Map all supported expiry for each pair.
// Search expiry and build pairsMap: USD-ETH@W -> USD-ETH@0705, USD-ETH@0712
// https://api.hbdm.com/api/v1/contract_contract_info should be pre-downloaded
var allContract = __dirname + "/../tmp/bncm_contract.json";
allContract = require('fs').readFileSync(allContract, 'utf8');
allContract = JSON.parse(allContract)['symbols'];
// Sort by symbol 'BTCUSD_210326' to make sure code Q2 is after code Q
allContract.sort((r1, r2) => { r1.symbol.localeCompare(r2.symbol); });
var contractMap = {};
allContract.forEach((r) => {
	if (r.contractStatus != 'TRADING') return;
	var expiryDate = null;
	var expiryCode = null;
	if (r.contractType == 'PERPETUAL') {
		expiryCode = 'P';
		expiryDate = 'P';
	} else if (r.contractType == 'CURRENT_QUARTER') {
		expiryDate = r.symbol.split('_')[1].substring(2); // 210326 -> 0326
		expiryCode = 'Q';
	} else if (r.contractType == "NEXT_QUARTER") {
		expiryDate = r.symbol.split('_')[1].substring(2); // 210326 -> 0326
		expiryCode = 'Q2';
	} else
		return; // Skip other types
	var pair = r.quoteAsset + '-' + r.baseAsset; // USD-BTC
	if (r.baseAsset + r.quoteAsset != r.symbol.split('_')[0]) { // BTC+USD -> BTCUSD_PERP
		console.log("Unexpected pair name", r.baseAsset, r.quoteAsset, r.symbol);
		return;
	}
	contractMap[pair + '@' + expiryCode] = {
		market_name : (pair + '@' + expiryDate),
		symbol : r.symbol
	}
});
console.log(contractMap);

// Global status
var pairsName = util.getCliPairs();
if (pairsName.length == 0)
	pairsName = ['USD-BTC@P', 'USD-BTC@Q'];
var newPairsName = [];
var pairsMap = {}; // BTCUSD_CQ -> USD-BTC@1227
// Search expiry and build pairsMap: USD-BTC@Q -> USD-BTC@0628, USD-BTC@0927
for (var pairWithEcode in contractMap) {
	var info = contractMap[pairWithEcode];
	var pairWithExpiry = info.market_name; // USD-BTC@0628
	var symbol = info.symbol; // BTCUSD_210628
	// Also select USD-BTC@Q2 if pairsName contains USD-BTC@Q
	var found = pairsName.filter((n) => {
		// console.log(pairWithExpiry, n, pairWithEcode.indexOf(n) == 0);
		return pairWithEcode.indexOf(n) == 0;
	}).length > 0;
	if (found == false) continue;
	// pairWithEcode hit with command args;
	console.log("Hit from command args", pairWithEcode);
	var basePair = info.market_name.split('@')[0]; // USD-BTC
	var expiryCode = pairWithEcode.split('@')[1];
	if (expiryCode == 'Q' && contractMap[basePair+'@Q'] != null) {
		newPairsName.push(contractMap[basePair+'@Q'].market_name);
		pairsMap[contractMap[basePair+'@Q'].symbol] = contractMap[basePair+'@Q'].market_name;
	} else if (expiryCode == 'Q2' && contractMap[basePair+'@Q2'] != null) {
		newPairsName.push(contractMap[basePair+'@Q2'].market_name);
		pairsMap[contractMap[basePair+'@Q2'].symbol] = contractMap[basePair+'@Q2'].market_name;
	} else if (expiryCode == 'P' && contractMap[basePair+'@P'] != null) {
		newPairsName.push(contractMap[basePair+'@P'].market_name);
		pairsMap[contractMap[basePair+'@P'].symbol] = contractMap[basePair+'@P'].market_name;
	} else {
		logger.info("Not supported expiry code: " + expiryCode);
		process.exit();
	}
}
pairsName = newPairsName;

console.log('pairsMap', pairsMap);
console.log('pairsName', pairsName);

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
var lastValidDepthMsgTime = Date.now();
var lastValidTradeMsgTime = Date.now();

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
	lastValidDepthMsgTime = Date.now();
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
	lastValidTradeMsgTime = Date.now();
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
		hostname: 'dapi.binance.com',
		path: '/dapi/v1/depth?limit=50&symbol='+symbol.toUpperCase(),
		headers: {
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
			'Host': 'dapi.binance.com',
			'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:72.0) Gecko/20100101 Firefox/72.0'
		},
		timeout:10*1000,
		times:1,
		interval:0
	};
	marketLog(market, "Requesting depth:" + JSON.stringify(options));
	var url = 'https://dapi.binance.com/dapi/v1/depth?limit=50&symbol='+symbol.toUpperCase();
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
	var past1 = Date.now() - lastValidTradeMsgTime;
	var past2 = Date.now() - lastValidDepthMsgTime;
	cgui.globalLog("last trade/depth: " + past1/1000 + "s/" + past2/1000 + "s");
	// Trade data could be much less than depth
	if (past1 < 1200*1000 && past2 < 120*1000)
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
	var url = 'wss://dstream.binance.com/ws/'+symbol.toLowerCase()+'@depth';
	logger.info('WSS Depth ' + market + ' -> ' + url);
	client.connect(url);
	client.on('connect', function(connection) {
		// logger.info('WebSocket depth[' + market + '] Connected');
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
			var actualMarket = pairsMap[message['s']];
			handleDepthUpdateData(actualMarket, message);
		});
	});
}
for (var symbol in pairsMap) {
	var market = pairsMap[symbol];
	var client = new WebSocketClient();
	var url = 'wss://dstream.binance.com/ws/'+symbol.toLowerCase()+'@aggTrade';
	logger.info('WSS Trade ' + market + ' -> ' + url);
	client.connect(url);
	client.on('connect', function(connection) {
		// logger.info('WebSocket trade[' + market + '] Connected');
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
			var actualMarket = pairsMap[message['s']];
			handleTradeData(actualMarket, message);
		});
	});
}
if (debug != true && pairsName.length < 4)
	cgui.start();
