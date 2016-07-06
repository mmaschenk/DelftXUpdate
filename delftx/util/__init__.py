import datetime
import os
import names
import gzip
import logging
import json
import traceback

from dateutil.parser import parse

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
        return open(bufFile, "r"), bufFile
    else:
        zfile = names.eventlogfile(date, base_path)
        logger.debug('Opening zipped file [%s]' % (zfile,))
        return gzip.GzipFile(zfile, "r"), zfile


def eventGenerator(course_metadata_map, base_path, bufferLocation=None):
    oneday = datetime.timedelta(days=1)

    current_date = parse(course_metadata_map["start_date"]).date()
    end_next_date = parse(course_metadata_map["end_date"]).date() + oneday

    while current_date < end_next_date:
        logger.debug("Opening logfile")
        input_file = openeventlogfile(current_date, base_path,
                                      bufferLocation)
        logger.debug("Opened logfile")
        lines = input_file.readlines()
        logger.debug("Read all input into memory. Processing %s lines" %
                     (len(lines),))
        first = True
        for line in lines:
            jsonObject = json.loads(line)
            yield first, jsonObject
            first = False
        current_date = current_date + oneday
    logger.debug("Finished all event files")


class filteringEventGenerator(object):

    def __init__(self, course_metadata_map, base_path,
                 bufferLocation=None):
        self.course_metadata_map = course_metadata_map
        self.base_path = base_path
        self.bufferLocation = bufferLocation

    def filteringEventGenerator(self):

        oneday = datetime.timedelta(days=1)

        current_date = parse(self.course_metadata_map["start_date"]).date()
        end_next_date = parse(
            self.course_metadata_map["end_date"]).date() + oneday

        logger.info('Going to proces files from: %s until %s' %
                    (current_date, end_next_date))
        course_id = self.course_metadata_map["course_id"]
        totalCnt = 0
        filteredCnt = 0
        while current_date < end_next_date:
            logger.debug("Opening logfile")
            input_file, filename = openeventlogfile(current_date,
                                                    self.base_path,
                                                    self.bufferLocation)
            self.currentfile = filename
            logger.debug("Opened logfile")
            first = True
            self.currentline = 0
            with input_file as eventloginput:
                for line in eventloginput:
                    self.currentline = self.currentline + 1
                    self.lastJsonObject = None
                    self.lastJsonObject = json.loads(line)
                    totalCnt = totalCnt + 1
                    if course_id in self.lastJsonObject["context"][
                            "course_id"]:
                        filteredCnt = filteredCnt + 1
                        yield first, self.lastJsonObject
                        first = False
            logger.debug("Finished file. Read %s lines" % (self.currentline,))
            logger.debug("Filtered %d from %d entries so far (%.2f %%)" %
                         (totalCnt - filteredCnt,
                          totalCnt,
                          float(totalCnt - filteredCnt) / totalCnt * 100))
            current_date = current_date + oneday
        logger.debug("Finished all event files. %s entries read. "
                     "%s entries remained after filtering"
                     % (totalCnt, filteredCnt))


class EventlogProcessor(object):

    def __init__(self, course_metadata_map, base_path, connection):
        super(EventlogProcessor, self).__init__()
        self.course_metadata_map = course_metadata_map
        self.base_path = base_path
        self.connection = connection

    def init_next_file(self):
        logger.warn('Unimplemented init_next_file method')

    def post_next_file(self):
        logger.debug('Unimplemented post_next_file method')

    def handleEvent(self, jsonObject):
        raise Exception(
            'handleevent not implemented on ', self.__class__.__name__)

    def postprocessing(self):
        logger.warn('Unimplemented postprocessing method')


class EventProcessorRunner():

    def __init__(self, course_metadata_map, base_path):
        self.processors = []
        self.course_metadata_map = course_metadata_map
        self.base_path = base_path

    def addProcessor(self, processor):
        self.processors.append(processor)

    def processall(self):
        evg = filteringEventGenerator(self.course_metadata_map, self.base_path)
        donestuff = False
        for p in self.processors:
            p.processstatistics = {'totallines': 0, 'totalerrors': 0}
        try:
            for firstevent, jsonObject in evg.filteringEventGenerator():
                if firstevent:
                    if donestuff:
                        logger.debug('Calling post_next_file on processors')
                        for p in self.processors:
                            p.post_next_file()
                        logger.debug('Done calling post_next_file on '
                                     'processors')
                        for p in self.processors:
                            logger.debug('Module %s: %s lines processed. '
                                         '%s errors' %
                                         (p.__class__,
                                          p.processstatistics['totallines'],
                                          p.processstatistics['totalerrors']))

                    logger.debug('Calling init_next_file on processors')
                    for p in self.processors:
                        p.init_next_file()
                    logger.debug('Done calling init_next_file on processors')
                for p in self.processors:
                    try:
                        p.processstatistics['totallines'] += 1
                        p.handleEvent(jsonObject)
                    except:
                        p.processstatistics['totalerrors'] += 1
                        logger.error('error occured in handleevent')
                        for line in traceback.format_exc().splitlines():
                            logger.error(line)
                        logger.error('error occured while processing file %s '
                                     'line %s' %
                                     (evg.currentfile, evg.currentline))
                        logger.error('Json object is %s' %
                                     (evg.lastJsonObject))
                        logger.error('processing continues but results may be '
                                     'incorrect')
                donestuff = True
            if donestuff:
                logger.debug('Calling final post_next_file on processors')
                for p in self.processors:
                    p.post_next_file()
                logger.debug('Done calling final post_next_file on processors')

            logger.debug('Calling postprocessing on processors')
            for p in self.processors:
                p.postprocessing()
            logger.debug('Done calling postprocessing on processors')
            for p in self.processors:
                logger.debug('Module %s: %s lines processed. %s errors' %
                             (p.__class__.__name__,
                              p.processstatistics['totallines'],
                              p.processstatistics['totalerrors']))
        except:
            logger.critical('error occured while processing file %s line %s' %
                            (evg.currentfile, evg.currentline))
            raise
