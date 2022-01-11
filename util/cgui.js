var colors = require('colors');
var pad = require('pad');
var moment = require('moment');

var util = require('../util/util.js')
var sortBuyOrder = util.sortBuyOrder;
var sortSellOrder = util.sortSellOrder;
var sortFilledOrder = util.sortFilledOrder;
var round = util.round;
var formatNum = util.formatNum;
var stringifyOrder = util.stringifyOrder;
var sortAndMergeOrderbook = util.sortAndMergeOrderbook;
var parseSize = util.parseSize;
var parseTime = util.parseTime;
var parsePrice = util.parsePrice;

var exp = module.exports;

// Global status
var exchange = '';
var needScreenUpdate = true;
var exchangesName = [];
var pairsName = [];
var pairsShownName = [];
var pairsNameInRedis = [];
var pairsMap = {};
var screenUpdateNum = 0;
var globalLogs = [];
var orderbookBids = {};
var orderbookAsks = {};
var mergedOrderbookBids = {};
var mergedOrderbookAsks = {};
var marketHistory = {};
var marketStatus = {};
var marketLogs = {};
var maxLogs = 100;
var ownOrders = {};
var marketTraderStatus = {};
var guiStarted = false;
var isViewer = false;

exp.init = function (options) {
	options = options || {};
	pairsName = options.pairsName || [];
	pairsShownName = options.pairsShownName || pairsName;
	pairsMap = options.pairsMap || {};
	for(var p in pairsMap) {
		orderbookBids[pairsMap[p]] = [];
		orderbookAsks[pairsMap[p]] = [];
		mergedOrderbookBids[pairsMap[p]] = [];
		mergedOrderbookAsks[pairsMap[p]] = [];
		marketHistory[pairsMap[p]] = [];
		marketStatus[pairsMap[p]] = '';
		marketLogs[pairsMap[p]] = [];
	}

	exchange = options.exchange || exchange;

	isViewer = (options.isViewer == true);
	exchangesName = options.exchangesName || [];
	if (isViewer == true && exchangesName.length == 0) {
		console.error("Viewer without target exchanges list.");
		process.exit(0);
	}
	if (isViewer == false && exchangesName.length > 0) {
		console.error("Non-viewer with target exchanges list.");
		process.exit(0);
	}
	// For viewer: BTC-ETH-Bittrex -> Bittrex
	if (isViewer == true)
		pairsNameInRedis = pairsName.map(p=>p.split('-').slice(0,2).join('-'));
	else
		pairsNameInRedis = pairsName;
}

exp.start = function() {
	guiStarted = true;
	setInterval(function(){
		drawScreen();
	}, 20);
	// Get latest live orders.
	setInterval(function(){
		util.traderStatus(pairsNameInRedis, function(err, res, _marketTraderStatus) {
			if (err == null) {
				try{
					var orders = {};
					for (var p in res) {
						orders[p] = {};
						for (var type in res[p]) {
							orders[p][type] = {};
							for (var i in res[p][type]) {
								var m = res[p][type][i]['market'].split('_')[0];
								orders[p][type][m] = orders[p][type][m] || [];
								orders[p][type][m].push(res[p][type][i]);
							}
						}
					}
					if (ownOrders != orders) needScreenUpdate = true;
					ownOrders = orders;
					marketTraderStatus = _marketTraderStatus;
				} catch (err) { console.error(err); }
			}
		});
	}, 400);
	// Adjust console size automatically.
	var lastCols = null;
	var lastRows = null;
	setInterval(function(){
		var stdout = process.stdout;
		var cols = stdout.columns;
		var rows = stdout.rows;
		if (cols == lastCols && rows == lastRows)
			return;
		lastCols = cols;
		lastRows = rows;
		needScreenUpdate = true;
	}, 100);
}

// Time that write to redis key/value snapshot
const lastMarketDataFlushTime = {};
exp.markUpdate = function(market, options) {
	// Increase ID
	var time = ('' + new Date()).split(' ')[4];
	var marketTime = options.marketTime;
	var latency = null;
	if (marketTime)
		latency = Math.round((Date.now() - marketTime)*100)/100;
	marketStatus[market] = marketStatus[market] || [null, 0, null];
	marketStatus[market][0] = time;
	marketStatus[market][1] += 1;
	var seriesNum = marketStatus[market][1];
	var odbkUpdated = false;
	var tickUpdated = false;
	var infoUpdated = false;
	var result = {};
	var maxDepth = options.maxDepth || 10;
	var maxTickHis = options.maxTickHis || 10;

	// Overwrite orderbook.
	if (options.orderbookBids != null) {
		orderbookBids[market] = options.orderbookBids;
		mergedOrderbookBids[market] = sortAndMergeOrderbook(
			exchange,
			market,
			orderbookBids[market],
			{'type':'bid', 'skip_sorting': options.skip_sorting}
		);
		odbkUpdated = true
	}
	if (options.orderbookAsks != null) {
		orderbookAsks[market] = options.orderbookAsks;
		mergedOrderbookAsks[market] = sortAndMergeOrderbook(
			exchange,
			market,
			orderbookAsks[market],
			{'type':'ask', 'skip_sorting': options.skip_sorting}
		);
		odbkUpdated = true;
	}
	if (options.info != null)
		infoUpdated = true;
	if (options.info != null && options.info['index'] != null)
		marketStatus[market][2] = options.info['index'];
	// Concat new trades.
	var fills = options['fills'] || [];
	// Boost mkt.rb parse_new_market_trades()
	// parsing new market trades from broadcast data directly.
	var newTrades = fills.map(o => {
		return {
			T : o.T.toLowerCase(),
			s : parseFloat(o.s),
			p : parseFloat(o.p),
			executed : o.s,
			remained : 0,
			status : 'filled',
			t : moment(o.t + "+0800").valueOf().toString()
		};
	});
	var tradeSnapshot = options['marketHistory'];
	if (tradeSnapshot != null) {
		marketHistory[market] = tradeSnapshot;
		result['marketHistory'] = marketHistory[market];
		tickUpdated = true;
	} else if (fills.length > 0) {
		// Merge, remove duplicated tuples. Keep as much as 999 for merging.
		// Only new trades is published in broadcastNew()
		// Only maxTickHis snapshot is published in broadcast()
		marketHistory[market] = marketHistory[market].concat(fills).sort(sortFilledOrder(exchange)).slice(0, 999);
		result['marketHistory'] = marketHistory[market];
		tickUpdated = true;
	}

	if (tickUpdated == false && odbkUpdated == false && infoUpdated == false) return result;
	needScreenUpdate = true;
	if (isViewer == true) return result; // Viewers do not broadcast update again.

	// Broadcast updates.
	var logHead = time + ' ID[' + seriesNum + '] ';
	var channelPrefix = "URANUS:" + exchange + ":" + market;
	var func_arg_2 = {
		'bids':mergedOrderbookBids[market].slice(0,maxDepth),
		'asks':mergedOrderbookAsks[market].slice(0,maxDepth),
		'info':options.info,
		'trades':marketHistory[market].slice(0, maxTickHis), // For flushing snapshot.
		'newFills': newTrades // For broadcasting new data directly.
	};
	var func_arg_3 = {
		'infoUpdated': infoUpdated,
		'tickUpdated': tickUpdated,
		'odbkUpdated': odbkUpdated,
		'marketTime' : options.marketTime
	};
	var flush = (Math.random() > 0.995) // Randomly log() and flush snapshot into redis sometimes.
	var lastSnapshotTime = lastMarketDataFlushTime[channelPrefix];
	if (lastSnapshotTime == null)
		lastSnapshotTime = 0;
	if (Date.now() - lastSnapshotTime > 10000) { // When last flush time is 10 seconds ago, flush.
		flush = true;
		lastMarketDataFlushTime[channelPrefix] = Date.now();
	}
	var func_cb = function (err, res) {
		if (err != null)
			return marketError(market, logHead + "data flush error" + err);
		// Print logs is high CPU consuming.
		if (guiStarted == false && flush == false) {
			;
		} else {
			var pubTime = res['pubTime'];
			var dataFlag = [(tickUpdated ? 'T' : ' '), (odbkUpdated ? 'O' : ' '), (infoUpdated ? 'I' : ' ')].join('');
			var log = logHead + " " + dataFlag + " P " + pubTime + "ms";
			if (res['flushTime']) log = log + " F " + res['flushTime'] + "ms";
			if (latency) log = log + " +" + latency + "ms";
			marketLog(market, log);
		}
	}
	util.broadcastNew(channelPrefix, func_arg_2, func_arg_3, func_cb);
	if (flush) // Also flush snapshot into redis sometimes.
		util.broadcast(channelPrefix, func_arg_2, func_arg_3, func_cb);

	return result;
}

///////////////////////////////////
// Log functions
///////////////////////////////////
exp.marketLog = marketLog = function (market, msg) {
	if (guiStarted == false) {
		console.info(exchange + '/' + market + ':' + msg);
	}
	marketLogs[market].unshift([msg]);
	marketLogs[market].slice(0, maxLogs);
}

exp.marketError = marketError = function (market, msg) {
	if (guiStarted == false)
		console.info(exchange + '/' + market + ':' + msg);
	marketLogs[market].unshift(['X', msg]);
	marketLogs[market].slice(0, maxLogs);
}

exp.globalLog = globalLog = function (...args) {
	if (guiStarted == false)
		return console.log.apply(console.log, args);
//	for (var m in marketLogs)
//		marketLog(m, message);
	var message = args[0];
	globalLogs.unshift(message);
	globalLogs.slice(0, maxLogs);
}

///////////////////////////////////
// GUI functions: general
///////////////////////////////////
function drawScreenCell(index, height, width) {
	// Prepare cell head info: abTrader balance/deviation.
	var exName = exchange;
	var market = pairsName[index];
	var mktName = market;
	// BTC-ETH-Bittrex -> Bittrex
	if (isViewer == true) {
		exName = market.split('-')[2];
		mktName = market.split('-').slice(0,2).join('-');
	}
	var abTraderPair = mktName;
	var info = marketTraderStatus[abTraderPair];
	var infoStr = '';
	if (info != null) {
		var exName = exName.split('@')[0];
		var balanceMap = info['balance'];
		var lowBalMkts = info['low_balance_market'];
		var deviationMap = info['deviation'];
		var bal = balanceMap[exName];
		if (bal == null)
			bal = 'X';
		else if (bal > 100)
			bal = Math.round(bal).toString();
		else if (bal > 0)
			bal = bal.toString().substring(0,4);
		else
			bal = Math.round(bal).toString();
		var deviationStr = 'X';
		var d = deviationMap[exName];
		if (d != null)
			deviationStr = Object.keys(d).map(k=>Math.round(d[k]*10000)).join(',');
		infoStr = bal + ' mt-bs:' + deviationStr;
		var opt = {};
		if (lowBalMkts.includes(exName))
			opt['low_bal'] = true;
		if (deviationStr == 'X')
			infoStr = null;
	}
	return generateScreenBuffer(pairsName[index], pairsShownName[index], height, width, infoStr, opt);
}
function ratio_char(r, normal) { //  ▁▃▄▅▆▇█
	if (r < 0.1)
		return '_';
	else if (r < 0.2)
		return '‗';
	else if (r < 0.3)
		return '≡';
	else if (r < 0.4)
		return '▃';
	else if (r < 0.5)
		return '▄';
	else if (r < 0.6)
		return '▅';
	else if (r < 0.7)
		return '▆';
	else if (r < 0.85)
		return '▇';
	return '█';
}
function drawScreenCellPostRender(index, bufferLines) {
	var pairName = pairsNameInRedis[index];
	var exName = exchange;
	if (isViewer == true) exName = exchangesName[index];
	var exName = exName.split('@')[0];
	var legacyOrders = [];
	var currentOrders = [];
	var childOrders = [];
	if (ownOrders[pairName] != null) {
		legacyOrders = ownOrders[pairName]['legacy'] || {};
		legacyOrders = legacyOrders[exName] || [];
		currentOrders = ownOrders[pairName]['current'] || {};
		currentOrders = currentOrders[exName] || [];
		childOrders = ownOrders[pairName]['child'] || {};
		childOrders = childOrders[exName] || [];
	}
	var newBuffer = [];
	// Render color after padding.
	for (var l in bufferLines) {
		var line = bufferLines[l];
		if (bufferLines[l].indexOf('BUY') >= 0)
			newBuffer.push(bufferLines[l].green);
		else if (bufferLines[l].indexOf('bid') >= 0)
			newBuffer.push(bufferLines[l].green);
		else if (bufferLines[l].indexOf('SEL') >= 0)
			newBuffer.push(bufferLines[l].red);
		else if (bufferLines[l].indexOf('ask') >= 0)
			newBuffer.push(bufferLines[l].red);
		else {
			// Render color of own orders.
			for (var i in currentOrders) {
				var o = currentOrders[i];
				var p = '' + o['p'] + ' ';
				if (o['p'] < 0.000001)
					p = '' + formatNum(o['p'], 1, 12).trim() + ' ';
				if (line.indexOf(p) >= 0) {
					var sum = line.split(p)[1].trim().split(' ')[0];
					sum = sum.replace('k','000');
					var size = 0;
					for (var j in currentOrders)
						if (currentOrders[j]['p'] == o['p'])
							size += currentOrders[j]['s'];
					var ratio = ratio_char(size/parseFloat(sum));
					var rp = '' + p.trim();
					if (o['T'] == 'buy')
						bufferLines[l] = line.replace(p, rp.green.inverse+ratio.green);
					else
						bufferLines[l] = line.replace(p, rp.red.inverse+ratio.red);
					line = bufferLines[l];
				}
			}
			for (var i in childOrders) {
				var o = childOrders[i];
				var p = '' + o['p'] + ' ';
				if (line.indexOf(p) >= 0) {
					var sum = line.split(p)[1].trim().split(' ')[0];
					sum = sum.replace('K','000');
					var size = 0;
					for (var j in childOrders)
						if (childOrders[j]['p'] == o['p'])
							size += childOrders[j]['s'];
					var ratio = ratio_char(size/parseFloat(sum));
					var rp = '' + p.trim();
					if (o['T'] == 'buy')
						bufferLines[l] = line.replace(p, rp.green+ratio.green);
					else
						bufferLines[l] = line.replace(p, rp.red+ratio.red);
					line = bufferLines[l];
				}
			}
			for (var i in legacyOrders) {
				var o = legacyOrders[i];
				var p = '' + o['p'] + ' ';
				if (o['p'] < 0.000001)
					p = '' + formatNum(o['p'], 1, 12).trim() + ' ';
				if (line.indexOf(p) >= 0) {
					var sum = line.split(p)[1].trim().split(' ')[0];
					sum = sum.replace('K','000');
					var size = 0;
					for (var j in legacyOrders)
						if (legacyOrders[j]['p'] == o['p'])
							size += legacyOrders[j]['s'];
					var ratio = ratio_char(size/parseFloat(sum));
					var rp = '' + p.trim();
					if (o['T'] == 'buy')
						bufferLines[l] = line.replace(p, rp.yellow.inverse+ratio.yellow);
					else
						bufferLines[l] = line.replace(p, rp.blue.inverse+ratio.blue);
					line = bufferLines[l];
				}
			}
			newBuffer.push(bufferLines[l]);
		}
	}
	newBuffer[0] = newBuffer[0].inverse;
	return newBuffer;
}
function drawScreen() {
	if (needScreenUpdate == false)
		return;
	needScreenUpdate = false;
	screenUpdateNum += 1;
	var past = Date.now();
	var buffer = splitScreenBuffer(
		pairsName.length,
		drawScreenCell,
		{
			hideHSplitLine:true,
			hideVSplitLine:true,
			postRenderFunc:drawScreenCellPostRender,
			logs: globalLogs
		}
	);
	// https://rosettacode.org/wiki/Terminal_control/Cursor_positioning#C.2FC.2B.2B
	// Using ANSI escape sequence, where ESC[y;xH moves curser to row y, col x:
	// Move CURSOR to 0,0
	// printf("\033[6;3HHello\n");
	process.stdout.write("\033[0;0H" + buffer);
	past = Date.now() - past;
	globalLog("Draw in " + past + "ms " + (new Date()));
}

function splitScreenBuffer(num, subDrawFunc, option) {
	option = option || {};
	var vgap = 1;
	var hgap = 1;
	if (option.hideVSplitLine == true)
		vgap = 0;
	if (option.hideHSplitLine == true)
		hgap = 0;
	var postRenderFunc = option.postRenderFunc;
	var stdout = process.stdout;
	var cols = stdout.columns;
	var rows = stdout.rows;
	var minCellWidth = option.minCellWidth || 48

	var cellWidth = Math.min(cols, minCellWidth);
	var cellNumHorizontal = Math.floor(cols/cellWidth);
	cellWidth = Math.floor(cols/cellNumHorizontal);
	var cellNumVertical = Math.ceil(num/cellNumHorizontal);
	var cellHeight = Math.floor(rows/cellNumVertical);
	if (cellNumHorizontal == 1) vgap = 0;

	// Compose screen buffer.
	var screenBuffer = "";
	var displayedRows = 0;
	for (var y=0; y<cellNumVertical; y=y+1) {
		var lines = null;
		for (var x=0; x<cellNumHorizontal; x=x+1) {
			var i = y*cellNumHorizontal + x;
			if (i >= num) break;
			var info = null;
			var subWidth = cellWidth;
			if (x < cellNumHorizontal-1)
				subWidth = cellWidth - vgap;
			var buffer = subDrawFunc(i, cellHeight-hgap, subWidth);
			var bufferLines = buffer.split("\n");
			if (x < cellNumHorizontal-1)
				for (var l in bufferLines)
					bufferLines[l] = pad(bufferLines[l], cellWidth-vgap);
			if (postRenderFunc != null)
				bufferLines = postRenderFunc(i, bufferLines);
			// Compose same line.
			if (lines == null)
				lines = bufferLines;
			else {
				if (option.hideVSplitLine != true)
					for (var l in lines)
						lines[l] += ('│' + bufferLines[l]);
				else
					for (var l in lines)
						lines[l] += bufferLines[l];
			}
		}
		displayedRows += lines.length;
		screenBuffer += lines.map(s => pad(s, cols)).join("\n"); // Fill screen width
		if (y < cellNumVertical-1) {
			screenBuffer += "\n";
			if (option.hideHSplitLine != true) {
				screenBuffer += pad('─', cols, '─');
				displayedRows += 1;
			}
		}
	}
	// Fill remain lines. Last line is always empty
	var remainRowNumber = rows - displayedRows - 1;
	var logs = option.logs || [];
	for (var i=0; i<=remainRowNumber; i++) {
		if (i == 0)
			screenBuffer += "\n";
		if (logs.length > i)
			screenBuffer += logs[i].slice(0, cols);
		else
			screenBuffer += "remain " + i;
		if (i < remainRowNumber)
			screenBuffer += "\n";
	}
	screenBuffer += "\r";
//	screenBuffer += "\r" + [cols, rows, cellWidth, cellHeight, remainRowNumber].join(',') + "\r";
	return screenBuffer;
}

///////////////////////////////////
// GUI functions: draw single cell
///////////////////////////////////
var MAX_ODBK_VALID_ROWS = process.env['URANUS_SPIDER_ODBK_MAX'];
function generateScreenBuffer(market, shownName, rows, cols, infoStr, opt={}) {
	var exName = exchange;
	var mktName = market;
	// BTC-ETH-Bittrex -> Bittrex
	if (isViewer == true) {
		exName = shownName.split('-')[2];
		mktName = shownName.split('-').slice(0,2).join('-');
	}
	var stringBuffer = "";
	var headline = exName + '/' + mktName + " " +
	   rows + "x" + cols + " " + (marketStatus[market][2] || '');
	if (infoStr != null)
		headline = exName + '/' + mktName + " " + infoStr;
	// Highlight future markets. Check the @
	// Highlight low bal markets.
	if (opt['low_bal'] == true) {
		headline = pad(headline, cols - 7);
		if (headline.indexOf('@') >= 0) headline = headline.yellow;
		headline = headline + 'LOW BAL'.red;
	} else {
		headline = pad(headline, cols);
		if (headline.indexOf('@') >= 0) headline = headline.yellow;
	}
	stringBuffer += headline + "\n";

	// Headline, logline
	var reservedLines = 1;
	var logLines = Math.max(1, Math.min(Math.floor(rows/10), 3));
	reservedLines += logLines;

	// 1 headline, then split into orderbook and trades.
	// Orderbook
	var odbkDisplayRows = Math.floor((rows-reservedLines)*3/4);
	if (odbkDisplayRows < 6)
		odbkDisplayRows = Math.min(10, rows-reservedLines-2);
	if (odbkDisplayRows < 3)
		odbkDisplayRows = Math.min(10, rows-reservedLines);
	var validOdbkRows = 0;
	var bids = mergedOrderbookBids[market].slice(0, odbkDisplayRows);
	var asks = mergedOrderbookAsks[market].slice(0, odbkDisplayRows);
	for (validOdbkRows = 1; validOdbkRows <= odbkDisplayRows; validOdbkRows++)
		if (bids[validOdbkRows-1]==null && asks[validOdbkRows-1]==null) break;
	// to keep orderbook shown size always.
	if (MAX_ODBK_VALID_ROWS != null)
		validOdbkRows = MAX_ODBK_VALID_ROWS;
	odbkDisplayRows = Math.min(odbkDisplayRows, validOdbkRows);
	reservedLines += odbkDisplayRows;
	// Occupy rows for empty trades.
	var remainLines = rows - reservedLines;
	var ticks = marketHistory[market]; // .slice(0, remainLines); Merge trades
	var validTickNum = 0;
	for (validTickNum = 0; validTickNum < remainLines; validTickNum ++)
		if (ticks[validTickNum] == null) break;
	if (remainLines > validTickNum)
		odbkDisplayRows += (remainLines - validTickNum);
	for (var i = 0; i < odbkDisplayRows; i++) {
		var line = '';
//		line +=(pad(i+'', 2) + "");
		line +=(stringifyOrder(exchange, bids[i]));
		if ((i+1)%10==0)
			line +=('-'); // Mark each 10 rows
		else
			line +=(' ');
		line +=(stringifyOrder(exchange, asks[i]));
		line = line.slice(0, cols);
		line +=("\n");
		stringBuffer += line;
	}

	// Trades rows.
	var mergedTradeNum = 0;
	var tradesShown = 0;
	var mergeTrade = true;
	var tradesStringBuffer = "";
	for (var i = 0; i < validTickNum+mergedTradeNum; i++) {
		if (ticks[i] == null) break;
		var mergedTick = { 's': parseFloat(ticks[i].s), 'p': ticks[i].p, 'T': ticks[i].T, 't': ticks[i].t };
		// show merged trades at same price.
		if (mergeTrade) {
			for (var j = i+1; j < validTickNum+mergedTradeNum; j++) {
				if (ticks[j] == null) break;
				if (ticks[i].p == ticks[j].p && ticks[i].T == ticks[j].T) {
					mergedTick.s += parseFloat(ticks[j].s);
					i = j;
					mergedTradeNum += 1;
				} else break;
			}
		}
		tradesStringBuffer +=(stringifyOrder(exchange, mergedTick, 'FIL'));
		tradesStringBuffer +=("\n");
		tradesShown += 1;
	}
	for (var i = 0; i < validTickNum - tradesShown; i++)
		stringBuffer +=("\n");
	stringBuffer += tradesStringBuffer;

	// Logs.
	var logs = marketLogs[market];
	for (var i = 0; i < logLines; i++) {
		if (i < logs.length)
			stringBuffer += logs[i].join(' ').slice(0, cols);
		else
			stringBuffer += "";
		if (i < logLines-1)
			stringBuffer +=("\n");
	}

	return stringBuffer;
}
