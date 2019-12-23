export BITTREX_API_DOMAIN='https://bittrex.com/api/v1.1'
export BITTREX_API_DOMAIN_V3='https://api.bittrex.com/v3'

export HITBTC_API_DOMAIN='https://api.hitbtc.com'

export LIQUI_API_DOMAIN='https://api.liqui.io/tapi'

export BFX_API_DOMAIN='https://api.bitfinex.com'

export BINANCE_API_DOMAIN='https://www.binance.com'
export BINANCE_API_DOMAIN_V3='https://api.binance.com'

export HUOBI_API_DOMAIN_TRADE='https://api.huobi.pro'
export HUOBI_API_DOMAIN_MARKET='https://api.huobi.pro'

export HBDM_API_DOMAIN='https://api.hbdm.com'

export OKEX_API_DOMAIN='https://www.okex.com/api'

export URANUS_WITHDRAW_MARKETS='BINANCE,BITTREX,HITBTC,HUOBI,BFX,OKEX,POLO,KRAKEN'
export URANUS_DEPOSIT_MARKETS="$URANUS_WITHDRAW_MARKETS",BITMEX

export ZB_API_DOMAIN_MATKET='https://api.zb.com/data'
export ZB_API_DOMAIN_TRADE='https://trade.zb.com/api'

export POLO_TRADING_API_DOMAIN='https://poloniex.com/tradingApi'
export POLO_PUBLIC_API_DOMAIN='https://poloniex.com/public'

export BITMEX_API_DOMAIN='https://www.bitmex.com'

export KRAKEN_API_DOMAIN='https://api.kraken.com'

export BITSTAMP_API_DOMAIN='https://www.bitstamp.net/api'

export BYBIT_API_DOMAIN='https://api.bybit.com'

export RUBY_VER='2.6.5'
# Setup Ruby JIT options from 2.6
# http://engineering.appfolio.com/appfolio-engineering/2018/4/3/ruby-26-and-ahead-of-time-compilation
[[ $RUBY_VER == 2.* ]] && \
	export JRUBY_OPTS='' && \
	export RUBYOPT='--jit --jit-min-calls=99999 --jit-max-cache=99999' && \
	export RUBYOPT=''

export NODE_VER='12'

if [ -z $DIR ]; then
	__DIR__="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
else
	__DIR__=$DIR
fi

[ -f  $__DIR__/conf/env2.sh ] && source $__DIR__/conf/env2.sh

if [[ $@ == *KEY* && -z $OKEX_ADDR_ZEN ]]; then
	if [ -f $__DIR__/conf/key.sh.gpg ]; then
		# GPG2 would wait for other gpg process and trigger a timeout.
		# Check other process first, ask a enter hitting.
		gpg_num=$( ps aux | grep 'gpg --cipher' | grep -v grep | wc -l )
		if [[ $gpg_num > 0 ]]; then
			echo "############ PRESS ENTER TO UNLOCK KEYS ############"
			read
		else
			echo "Unlock keys now"
		fi
		sudo killall -9 gpg-agent 2>/dev/null
		eval $( gpg --cipher-algo AES256 -d $__DIR__/conf/key.sh.gpg 2>/dev/null )
		sudo killall -9 gpg-agent 2>/dev/null
	else
		echo "No key conf at $__DIR__/conf/"
		exit 1
	fi
fi
