import mysql.connector
import argparse
import logging
import traceback

from delftx.util import courseinformation, names, EventProcessorRunner
from delftx import learnermode, forummode, videomode, quiz_mode

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


def processcourse(coursename, base_path, bufferLocation=None):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(filename)s '
                        '%(funcName)s %(message)s')
    infoname = names.course_structure_file(coursename, base_path)
    ci = courseinformation.extract(infoname)
    connection = opendb()

    epr = EventProcessorRunner(ci, base_path)
    learnersessions = learnermode.Sessions(ci, base_path, connection)
    epr.addProcessor(learnersessions)

    videointeraction = videomode.Sessions(ci, base_path, connection)
    epr.addProcessor(videointeraction)

    forumSession = forummode.Sessions(ci, base_path, connection)
    epr.addProcessor(forumSession)

    quizmode = quiz_mode.Assessments(ci, base_path, connection)
    epr.addProcessor(quizmode)

    quizsessions = quiz_mode.Sessions(ci, base_path, connection)
    epr.addProcessor(quizsessions)

    epr.processall()

    learnermode.process(coursename, base_path, connection, ci)
    forummode.process(coursename, base_path, ci, connection)


###############################################################################
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(filename)s '
                        '%(funcName)s %(message)s')

    parser = argparse.ArgumentParser(description='Process DelftX datafiles')
    parser.add_argument('coursename', type=str, default="")
    parser.add_argument('--directory', type=str, default="")
    parser.add_argument('--bufferlocation', type=str, default=None)
    parser.add_argument('--logfile', type=str, default=None)
    args = parser.parse_args()

    if args.logfile:
        fh = logging.FileHandler(filename=args.logfile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s '
                                      '%(funcName)s %(message)s')
        fh.setFormatter(formatter)
        logging.getLogger('').addHandler(fh)

    try:
        processcourse(args.coursename, args.directory,
                      bufferLocation=args.bufferlocation)
    except:
        for line in traceback.format_exc().splitlines():
            logger.critical(line)
        raise

    print "All finished."
