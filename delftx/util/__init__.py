import datetime
import os
import names
import gzip
import logging

logger = logging.getLogger(__name__)


def getNextDay(current_day_string):
    fmt = "%Y-%m-%d"
    current_day = datetime.datetime.strptime(current_day_string, fmt)
    oneday = datetime.timedelta(days=1)
    next_day = current_day + oneday
    return str(next_day)[0:10]


def openeventlogfile(date, base_path, bufferLocation=None):

    if bufferLocation:
        bufFile = os.path.join(bufferLocation, date.isoformat() + '.log')
        if not os.path.isfile(bufFile):
            zfile = names.eventlogfile(date, base_path)
            logger.debug('Buffering zipped file [%s from %s]' %
                        (bufFile, zfile))
            gz_file = gzip.GzipFile(zfile)
            bf = open(bufFile, "w")
            bf.write(gz_file.read())
            gz_file.close()
            bf.close()
        logger.debug('Opening unzipped file [%s]' % (bufFile,))
        return open(bufFile, "r")
    else:
        zfile = names.eventlogfile(date, base_path)
        logger.debug('Opening zipped file [%s]' % (zfile,))
        return gzip.GzipFile(zfile, "r")
