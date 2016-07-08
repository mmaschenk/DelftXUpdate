#!/usr/bin/env python

import argparse
import logging
import traceback

import delftx.util

logger = logging.getLogger(__name__)

###############################################################################
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(filename)s '
                        '%(funcName)s %(message)s')

    parser = argparse.ArgumentParser(description='Process DelftX datafiles')
    parser.add_argument('coursename', type=str, default="", nargs='+')
    parser.add_argument('--directory', type=str, default="")
    parser.add_argument('--bufferlocation', type=str, default=None)
    parser.add_argument('--logfile', type=str, default=None)
    parser.add_argument('--skipsessions', action='store_true')
    parser.add_argument('--skipmetadata', action='store_true')
    args = parser.parse_args()

    if args.logfile:
        fh = logging.FileHandler(filename=args.logfile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s '
                                      '%(funcName)s %(message)s')
        fh.setFormatter(formatter)
        logging.getLogger('').addHandler(fh)

    try:
        for course in args.coursename:
            print 'Doing course:', course
            delftx.util.processcourse(course, args.directory,
                                      bufferLocation=args.bufferlocation,
                                      skipSessions=args.skipsessions,
                                      skipMetadata=args.skipmetadata)
    except:
        for line in traceback.format_exc().splitlines():
            logger.critical(line)
        raise

    print "All finished."
