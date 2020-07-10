#!/bin/bash

USAGE=$(cat << EOF
muse-tool [muse|multi-muse|muse-merge]
	--help: Print this message
EOF
)
case "$1" in
	muse) muse "${@:2}";;
	multi-muse) /bin/multi_muse.py "${@:2}";;
	merge-muse) /bin/merge_muse.py "${@:2}";;
	*help) echo "$USAGE";;
esac
