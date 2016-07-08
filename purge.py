#!/usr/bin/env python

import mysql.connector
import argparse
import logging
import traceback

import delftx.util

logger = logging.getLogger(__name__)


def opendb():
    # Database
    user = 'root'
    password = ''
    host = '127.0.0.1'
    database = 'DelftX'
    connection = mysql.connector.connect(
        user=user, password=password, host=host, database=database,
        charset='utf8', use_unicode=True)
    return connection


###############################################################################
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(filename)s '
                        '%(funcName)s %(message)s')

    parser = argparse.ArgumentParser(description='Purge DelftX datafiles')
    parser.add_argument('coursename', type=str, default="")
    parser.add_argument('--directory', type=str, default="")
    parser.add_argument('--logfile', type=str, default=None)
    args = parser.parse_args()

    if args.logfile:
        fh = logging.FileHandler(filename=args.logfile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s '
                                      '%(funcName)s %(message)s')
        fh.setFormatter(formatter)
        logging.getLogger('').addHandler(fh)

    try:
        delftx.util.purgecourse(args.coursename, args.directory)
    except:
        for line in traceback.format_exc().splitlines():
            logger.critical(line)
        raise

    print "All finished."
