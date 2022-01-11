#!/bin/bash --login
SOURCE="${BASH_SOURCE[0]}"
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

[ -z $1 ] && echo "Exchange should be provided in arguments." && exit 1

cd $DIR
source $DIR/conf/env.sh
spider=''
pre_cmd=''
mode='spider'
if [[ $1 == 'bittrex' ]]; then
	spider=spider/bittrex_wss_v3.js
elif [[ $1 == 'hitbtc' ]]; then
	spider=spider/hitbtc_v3_wss.js
elif [[ $1 == 'liqui' ]]; then
	spider=spider/liqui_rest.js
elif [[ $1 == 'bfx' ]]; then
	spider=spider/bfx_wss.js
elif [[ $1 == 'binance' ]]; then
	spider=spider/binance_wss.js
elif [[ $1 == 'huobi' ]]; then
	spider=spider/huobipro_ws.js
elif [[ $1 == 'gate' ]]; then
	spider=spider/gate_rest.js
elif [[ $1 == 'zb' ]]; then
	spider=spider/zb_wss.js
elif [[ $1 == 'kucoin' ]]; then
	spider=spider/kucoin_rest.js
elif [[ $1 == 'okex' ]]; then
	spider=spider/okex_wss_v3.js
elif [[ $1 == 'polo' ]]; then
	spider=spider/poloniex_wss.js
elif [[ $1 == 'view' || $1 == 'viewer' ]]; then
	spider=spider/viewer.js
	mode='viewer'
elif [[ $1 == 'bitstamp' ]]; then
	spider=spider/bitstamp_wss_v2.js
elif [[ $1 == 'bncm' ]]; then
	spider=spider/binance_coinm_wss.js
	pre_cmd="mkdir -p $DIR/tmp && curl 'https://dapi.binance.com/dapi/v1/exchangeInfo' > $DIR/tmp/bncm_contract.json"
elif [[ $1 == 'bnum' ]]; then
	spider=spider/binance_usdtm_wss.js
	pre_cmd="mkdir -p $DIR/tmp && curl 'https://fapi.binance.com/fapi/v1/exchangeInfo' > $DIR/tmp/bnum_contract.json"
elif [[ $1 == 'bitmex' ]]; then
	spider=spider/bitmex_wss.js
	pre_cmd="mkdir -p $DIR/tmp && curl 'https://www.bitmex.com/api/v1/instrument/active' > $DIR/tmp/bitmex_contract.json"
elif [[ $1 == 'kraken' ]]; then
	spider=spider/kraken_wss.js
elif [[ $1 == 'kraken'* ]]; then # kraken1 kraken2 ...
	spider=spider/kraken_wss.js
elif [[ $1 == 'hbdm' ]]; then
	spider=spider/hbdm_wss.js
	pre_cmd="mkdir -p $DIR/tmp && curl 'https://api.hbdm.com/api/v1/contract_contract_info' > $DIR/tmp/hbdm_contract.json"
elif [[ $1 == 'bybit' ]]; then
	export uranus_spider_exchange=Bybit
	spider=spider/bybit_wss.js
elif [[ $1 == 'bybitu' ]]; then
	export uranus_spider_exchange=BybitU
	spider=spider/bybit_wss.js
elif [[ $1 == 'bybits' ]]; then
	spider=spider/bybitspot_v2_wss.js
elif [[ $1 == 'gemini' ]]; then
	spider=spider/gemini_wss_v2.js
elif [[ $1 == 'coinbase' ]]; then
	spider=spider/coinbase_wss.js
elif [[ $1 == 'ftx' ]]; then
	spider=spider/ftx_wss.js
elif [[ $1 == 'ftx'* ]]; then # ftx1 ftx2 ...
	spider=spider/ftx_wss.js
elif [[ $1 == 'x' ]]; then
	spider=spider/x.js
elif [[ $1 == 'kill' ]]; then
	acquire_pslock
	ps aux | grep node\.*spider\/ | grep -v grep
	echo "Press enter to kill above processes"
	read confirm
	ps aux | grep node\.*spider\/ | grep -v grep | awk '{ print $2 }' | xargs kill
	release_pslock
	exit 0
elif [[ $1 == 'killallspidersfullauto' ]]; then # Use a long name to avoid manually call
	acquire_pslock
	ps aux | grep node\.*spider\/ | grep -v grep | awk '{ print $2 }' | xargs kill
	release_pslock
	exit 0
else
	echo "Unknown exchange: $1"
	exit 1
fi

if [ -z $2 ] ; then
	# echo "Markets should be provided in arguments." && exit 1
	echo "No args, parse tmux_uranus.sh to get default spider args"
	line=$( grep " $1 " $DIR/bin/tmux_uranus.sh | grep spider.sh )
	echo -e "Match line:\n$line"
	[[ -z $line ]] && echo "No match line" && exit
	# Only get content between first and second quotes, then
	# Remove first 2 columns: $DIR/spider.sh market_name
	line=$( echo $line | awk -F '"' '{ print $2 }' | awk '{$1="";$2="";print }' | xargs )
	echo -e "Target args:\n$line"
	node_args=$line
	# For auto task in tmux_uranus.sh,
	# set URANUS_SPIDER_ODBK_MAX for less CPU usage
	if [[ $1 == 'bybit'* || $1 == 'bncm'* || $1 == 'bnum'* ]]; then
		export URANUS_SPIDER_ODBK_MAX=3 # Fewer memory for futures markets
	else
		export URANUS_SPIDER_ODBK_MAX=9
	fi
else
	# For mannual pairs mode, set URANUS_SPIDER_ODBK_MAX for better view
	export URANUS_SPIDER_ODBK_MAX=14
	shift
	node_args=$@
fi

# Check if bashrc need to be executed for nvm
type nvm > /dev/null || source ~/.bashrc
nvm use $NODE_VER


function loop_work {
	while true ; do
		cd $DIR
		[[ ! -z $pre_cmd ]] && echo $pre_cmd && eval $pre_cmd
		# Only in viewer mode, trap INT signal then do loop work again.
		[[ $mode == 'viewer' ]] && echo "Set trap INT" && trap interrupted INT
		# Unset sensitive variables. node.js might dump them when crash.
		(
			for var in $( compgen -e ); do
				if [[ $var == *_API_* ]]; then
					unset $var
				elif [[ $var == *_ADDR_* ]]; then
					unset $var
				elif [[ $var == URANUS_SPIDER_ODBK_MAX ]]; then
					continue # Keep this
				elif [[ $var == URANUS_* ]]; then
					unset $var
				fi
			done
			# 512m lead to a lot of out of heap error
			node --max_old_space_size=1024 $spider $node_args
		)
		echo "Spider is terminated, restart soon"
		trap - INT # Allow user to exit here
		sleep 1
	done
}

# trap INT signal then do loop work again.
function interrupted {
	echo "Interrupted, sleep 1s then do loop_work()"
	trap - INT # Allow user to exit here
	sleep 1
	trap interrupted INT
	loop_work
}

loop_work $node_args
