#!/bin/bash

USAGE=$(cat << EOF
muse-tool [muse|multi-muse|muse-merge]
	--help: Print this message
EOF
)
case "$1" in
	muse) muse "${@:2}";;
	multi-muse) /bin/multi-muse "${@:2}";;
	muse-merge) /bin/muse-merge "${@:2}";;
	*help) echo "$USAGE";;
esac
