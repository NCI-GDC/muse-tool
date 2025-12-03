#!/usr/bin/env python3
import sys

from muse_tool import multi_muse

if __name__ == "__main__":
    # CLI Entrypoint.
    retcode = 0

    try:
        retcode = multi_muse.main()
    except Exception as e:
        retcode = 1

    sys.exit(retcode)

# __END__
