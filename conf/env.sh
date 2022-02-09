#!/bin/bash
export OS=$( uname )

export URANUS_ENV_VER='20200219'

export BITTREX_API_DOMAIN='https://bittrex.com/api/v1.1'
export BITTREX_API_DOMAIN_V3='https://api.bittrex.com/v3'

export HITBTC_API_DOMAIN='https://api.hitbtc.com'

export LIQUI_API_DOMAIN='https://api.liqui.io/tapi'

export BFX_API_DOMAIN='https://api.bitfinex.com'

export BINANCE_API_DOMAIN_V3='https://api.binance.com'
export BINANCE_API_DOMAIN_CM='https://dapi.binance.com'
export BINANCE_API_DOMAIN_UM='https://fapi.binance.com'

export HUOBI_API_DOMAIN_TRADE='https://api.huobi.pro'
export HUOBI_API_DOMAIN_MARKET='https://api.huobi.pro'

export HBDM_API_DOMAIN='https://api.hbdm.com'

export FTX_API_DOMAIN='https://ftx.com'

export OKEX_API_DOMAIN='https://www.okex.com/api'

export URANUS_WITHDRAW_MARKETS='COINBASE,BINANCE,BITTREX,HITBTC,HUOBI,BFX,OKEX,POLO,KRAKEN,FTX'
export URANUS_DEPOSIT_MARKETS="$URANUS_WITHDRAW_MARKETS",BITMEX,BITSTAMP

export ZB_API_DOMAIN_MATKET='https://api.zb.com/data'
export ZB_API_DOMAIN_TRADE='https://trade.zb.com/api'

export POLO_TRADING_API_DOMAIN='https://poloniex.com/tradingApi'
export POLO_PUBLIC_API_DOMAIN='https://poloniex.com/public'

export BITMEX_API_DOMAIN='https://www.bitmex.com'

export KRAKEN_API_DOMAIN='https://api.kraken.com'

export BITSTAMP_API_DOMAIN='https://www.bitstamp.net/api'

export BYBIT_API_DOMAIN='https://api.bybit.com'

export GEMINI_API_DOMAIN='https://api.gemini.com'

export COINBASE_PRO_API_DOMAIN='https://api.pro.coinbase.com'
export COINBASE_API_DOMAIN='https://api.coinbase.com'

export TWS_GATEWAY_NAME=zwyang
export IB_ACCOUNT=U8620103

export RUBY_VER='3.0.0'
# export RUBY_VER='jruby'
# Tuning JRuby
# https://github.com/jruby/jruby/wiki/PerformanceTuning
jruby_profile_arg='-J-Dcom.sun.management.jmxremote -J-Dcom.sun.management.jmxremote.local.only=false -J-Dcom.sun.management.jmxremote.port=2001 -J-Dcom.sun.management.jmxremote.ssl=false -J-Dcom.sun.management.jmxremote.authenticate=false'
[[ $RUBY_VER == jruby* ]] && \
	export JRUBY_OPTS="-J-server -J-Xmx1024m -J-Xms1024m $jruby_profile_arg" && \
	export RUBYOPT=''

# Setup Ruby JIT options from 2.6
# http://engineering.appfolio.com/appfolio-engineering/2018/4/3/ruby-26-and-ahead-of-time-compilation
# [[ $RUBY_VER == 2.* ]] && \
# 	export JRUBY_OPTS='' && \
# 	export RUBYOPT='--jit --jit-min-calls=99999 --jit-max-cache=99999' && \
# 	export RUBYOPT=''

export NODE_VER='12'

if [ -z $DIR ]; then
	if [ -z $SOURCE ]; then
		__SOURCE__="${BASH_SOURCE[0]}"
		__DIR__="$( dirname "$__SOURCE__" )"/../
	else
		__DIR__="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
	fi
else
	__DIR__=$DIR
fi

#################################################################
# Too many ps aux would lead to high CPU competetion.
#################################################################
function try_pslock {
	ps_lock="./ps.lock"
	[ ! -z $DIR ] && ps_lock="$DIR/ps.lock"
	sleep_s=$( ruby -e 'puts (1+3*Random.rand).round(2)' ) # float 1~4
	echo "[$SOURCE] try_pslock wait $sleep_s for $ps_lock"
	sleep $sleep_s
	[ ! -f $ps_lock ] && touch $ps_lock && return 0
	echo "try_pslock failed"
	return 1
}

function release_pslock {
	ps_lock="./ps.lock"
	[ ! -z $DIR ] && ps_lock="$DIR/ps.lock"
	rm -f $ps_lock
	return 0
}

function acquire_pslock {
	# Shortcut for sourcing env.sh and exec.sh
	[[ $__SOURCE__ == */env.sh ]] && echo 'skip acquire_pslock' && return 0
	[[ $SOURCE == */exec.sh ]] && echo 'skip acquire_pslock' && return 0
	ps_lock="./ps.lock"
	[ ! -z $DIR ] && ps_lock="$DIR/ps.lock"
	while true ; do
		sleep_s=$( ruby -e 'puts (1+3*Random.rand).round(2)' ) # float 1~4
		echo "[$SOURCE] acquire_pslock wait $sleep_s for $ps_lock"
		sleep $sleep_s
		[ ! -f $ps_lock ] && touch $ps_lock && break
	done
	return 0
}

#################################################################
# Unlock KEY env if needed.
# Use ps aux to detect and wait for GPG password UI
#################################################################

if [[ $@ == *KEY* && -z $OKEX_ADDR_ZEN ]]; then
	gpg_f=$__DIR__/conf/key.sh.gpg
	echo "checking [$gpg_f]"
	if [[ -f $gpg_f ]]; then
		# GPG2 would wait for other gpg process and trigger a timeout.
		# Check other process first, ask a enter hitting.
		# xargs trims string for macos
		acquire_pslock
		gpg_num=$( ps aux | grep 'gpg --cipher' | grep -v grep | wc -l | xargs )
		release_pslock
		if [[ $gpg_num != 0 ]]; then
			echo "############ PRESS ENTER TO UNLOCK KEYS ############"
			read
		else
			echo "Unlock keys now"
		fi
		if [[ $OS == 'Darwin' ]]; then
			killall -9 gpg-agent 2>/dev/null
			eval $( gpg --cipher-algo AES256 -d $gpg_f 2>/dev/null )
			killall -9 gpg-agent 2>/dev/null
		else
			sudo killall -9 gpg-agent 2>/dev/null
			eval $( gpg --cipher-algo AES256 -d $gpg_f 2>/dev/null )
			sudo killall -9 gpg-agent 2>/dev/null
		fi
		# Change bash PS1 to alert user.
		[[ ! -z $OKEX_ADDR_ZEN ]] && export PS1="[! URN KEY !] $PS1"
	else
		echo "No key conf at $gpg_f press enter to exit"
		read
		exit 1
	fi
fi

# Temporary fix for $script conflix between bin/arbitrage_base.sh and rvm
[[ $script == override_gem ]] && export script=trader/ab3.rb

#################################################################
# For different OSes, init uranus_archive_old_logs()
# Would be invoked before arbitrage tasks.
# Use ps aux to detect and wait for its turn.
#################################################################
export URANUS_LOG_ARCHIVE_DIR="$__DIR__/log/archive"
export URANUS_LOG_DIR="$__DIR__/log"
if [[ $OS == 'Darwin' ]]; then
	# Only use external directory when disk is still there.
	# Use 250GB first, if missing then 320GB
	export URANUS_DARWIN_EXT_DIR='/Volumes/250GB/uranus'
	[ ! -d $URANUS_DARWIN_EXT_DIR ] && export URANUS_DARWIN_EXT_DIR='/Volumes/320GB/uranus'
	if [ -d $URANUS_DARWIN_EXT_DIR ] ; then
		export URANUS_LOG_ARCHIVE_DIR="$URANUS_DARWIN_EXT_DIR/log_archive"
		export URANUS_LOG_DIR="$URANUS_DARWIN_EXT_DIR/log"
		mkdir $URANUS_LOG_ARCHIVE_DIR && mkdir $URANUS_LOG_DIR
	else
                echo "No URANUS_DARWIN_EXT_DIR $URANUS_DARWIN_EXT_DIR , use default dir instead"
	fi
fi
mkdir -p "$URANUS_LOG_ARCHIVE_DIR"

export URANUS_RAMDISK=
if [[ $OS == 'Linux' ]]; then
	ram_disk=$( df -h | grep RAMDisk | wc -l )
	if [[ $ram_disk -gt 0 ]]; then
		df -h | grep RAMDisk
		ramdisk=$( df -h | grep RAMDisk | awk '{ print $6 }' )
		echo "Ramdisk exist at $ramdisk"
	fi
	export URANUS_RAMDISK=$ramdisk
elif [[ $OS == 'Darwin' ]]; then
	ram_disk=$( df -h | grep RAMDisk | wc -l )
	if [[ $ram_disk -gt 0 ]]; then
		df -h | grep RAMDisk
		ramdisk=$( df -h | grep RAMDisk | awk '{ print $9 }' )
		echo "Ramdisk exist at $ramdisk"
	fi
	export URANUS_RAMDISK=$ramdisk
fi

function uranus_archive_old_logs {
	for log in $1*.log ; do
		[ ! -f $log ] && continue
		basename=$( basename "$log" )
		gzname="$basename".gz
		while true ; do # Too many 'ps aux' leads to CPU competetion.
			try_pslock || continue
			sleep_s=$( ruby -e 'puts (1+3*Random.rand).round(2)' ) # float 1~4
			echo "Sleep $sleep_s then check other gzip processes."
			sleep $sleep_s
			echo "checking other gzip processes."
			gzip_process_num=$( ps aux | grep gzip | grep -v grep | wc -l | xargs )
			if [[ $gzip_process_num > 1 ]]; then
				echo "Too many gzip procs"
				ps aux | grep gzip | grep -v grep
				release_pslock
				continue
			else
				release_pslock
				break
			fi
		done
		ls -ahl  "$log"
		ramdisk_avail=0
		[ ! -z $URANUS_RAMDISK ] && ramdisk_avail=$( df | grep RAMDisk | awk '{ print $4 }' )
		if [ $ramdisk_avail -lt 300000 ]; then # If ramdisk available less than 300MB
			echo " > $URANUS_LOG_ARCHIVE_DIR/$gzname"
			cat "$log" | gzip --best > "$URANUS_LOG_ARCHIVE_DIR"/"$gzname"
			if [[ $? == 0 ]] ; then
				rm $log
				continue
			fi
		else # Compress to ramdisk then move to archive place.
			echo " > $URANUS_RAMDISK/$gzname"
			cat "$log" | gzip --best > "$URANUS_RAMDISK"/"$gzname"
			if [[ $? == 0 ]] ; then
				echo mv "$URANUS_RAMDISK"/"$gzname" "$URANUS_LOG_ARCHIVE_DIR"/"$gzname"
				mv "$URANUS_RAMDISK"/"$gzname" "$URANUS_LOG_ARCHIVE_DIR"/"$gzname"
				if [[ $? == 0 ]] ; then
					rm $log
					continue
				fi
			fi
		fi
		echo "Failed, press enter to continue"
		read
	done
}
export HOSTNAME # For sending email

# Do dynamic API filtering for Bitstamp in env2
[ -f  $__DIR__/conf/env2.sh ] && source $__DIR__/conf/env2.sh
[ -d  $__DIR__/tmp ] || mkdir $__DIR__/tmp
