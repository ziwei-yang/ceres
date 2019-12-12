#!/bin/bash --login
SOURCE="${BASH_SOURCE[0]}"
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

file=$1
[ -f $file ] || exit 1

source $DIR/conf/env.sh
rvm use $RUBY_VER # Setup Ruby env.
basename=$( basename $file )
if [ $basename == 'print_legacy.rb' ] || \
	[ $basename == 'print_xfr.rb' ] || \
	[ $basename == 'trader_report.rb' ] || \
	[ $basename == 'send_cmd.rb' ] || \
	[ $basename == 'reload_active_progress.rb' ] || \
	[ $basename == 'reload_legacy_progress.rb' ] || \
	[ $basename == 'future_spot.rb' ] || \
	[ $basename == 'fix_state.rb' ] || \
	[ $basename == 'save_mkt.rb' ] || \
	[ $basename == 'filter.rb' ] || \
	[ $basename == 'test.rb' ]
then
	source $DIR/conf/dummy_env.sh
	source $DIR/conf/env.sh
elif [ $basename == 'dmy.rb' ] || \
	[ $basename == 'data.rb' ] || \
	[ $basename == 'mars.rb' ]
then
	if [[ $@ == *live* ]]; then
		source $DIR/conf/env.sh KEY
	elif [[ $@ == *.gz* ]]; then
		source $DIR/conf/env.sh KEY
	else
		source $DIR/conf/dummy_env.sh
		source $DIR/conf/env.sh
	fi
else
	source $DIR/conf/env.sh KEY
fi
shift
ruby $file $@
