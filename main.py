import mysql.connector
import argparse
import logging


from delftx.util import courseinformation, names, EventProcessorRunner
from delftx import learnermode, forummode, videomode, quiz_mode


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

    parser = argparse.ArgumentParser(description='Process DelftX datafiles')
    parser.add_argument('coursename', type=str, default="")
    parser.add_argument('--directory', type=str, default="")
    parser.add_argument('--bufferlocation', type=str, default=None)
    args = parser.parse_args()
    processcourse(args.coursename, args.directory,
                  bufferLocation=args.bufferlocation)
    print "All finished."
