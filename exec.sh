#!/bin/bash --login
SOURCE="${BASH_SOURCE[0]}"
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

file=$1
[ -f $file ] || exit 1
shift

source $DIR/conf/env.sh
rvm use $RUBY_VER # Setup Ruby env.
basename=$( basename $file )
need_log=0
echo $basename
if [ $basename == 'print_legacy.rb' ] || \
	[ $basename == 'print_xfr.rb' ] || \
	[ $basename == 'print_algo.rb' ] || \
	[ $basename == 'print_state.rb' ] || \
	[ $basename == 'trader_report.rb' ] || \
	[ $basename == 'send_cmd.rb' ] || \
	[ $basename == 'reload_active_progress.rb' ] || \
	[ $basename == 'reload_legacy_progress.rb' ] || \
	[ $basename == 'future_spot.rb' ] || \
	[ $basename == 'fix_state.rb' ] || \
	[ $basename == 'quickfix.rb' ] || \
	[ $basename == 'save_mkt.rb' ] || \
	[ $basename == 'filter.rb' ] || \
	[ $basename == 'api.rb' ] || \
	[ $basename == 'ib.rb' ] || \
	[ $basename == 'test.rb' ] || \
	[ $basename == 't.rb' ] ; then
	source $DIR/conf/dummy_env.sh
	source $DIR/conf/env.sh
elif [ $basename == 'pos.rb' ] ; then
	source $DIR/conf/env.sh KEY
elif [ $basename == 'dmy.rb' ] || \
	[ $basename == 'data.rb' ] || \
	[[ $file == trader/*.rb ]] || \
	[[ $file == signal/*.rb ]] ; then
	if [[ $@ == *live* ]]; then
		source $DIR/conf/env.sh KEY
		echo "For live HFT algos, enable logs"
		need_log=1
	elif [[ $@ == *.gz* ]]; then
		source $DIR/conf/env.sh KEY
	else
		source $DIR/conf/dummy_env.sh
		source $DIR/conf/env.sh
	fi
elif [ $basename == 'agt.rb' ] ; then
	source $DIR/conf/env.sh KEY
	need_log=1
else
	source $DIR/conf/env.sh KEY
fi

rm -f $DIR/Gemfile.lock
if [[ $need_log == 0 ]]; then
	ruby $file $@
else
	datetime=`date +"%Y_%m_%d_%H_%M"`
	log_file="$DIR/log/$basename_$datetime.log"
	log_file="$DIR/log/$basename.log"
	echo "unbuffer -p ruby $file $@ 2>&1 | tee $log_file"
	unbuffer -p ruby $file $@ 2>&1 | tee $log_file
fi
