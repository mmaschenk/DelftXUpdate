'''
Created on Jun 18, 2016

@author: Angus
'''

import os
import gzip
import mysql.connector
import json
import argparse
import logging

from translation.LearnerMode import learner_mode, sessions
from translation.ForumMode import forum_interaction, forum_sessions
from translation.VideoMode import video_interaction
from translation.QuizMode import quiz_mode, quiz_sessions

from delftx.util import courseinformation, names
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
                        format='%(asctime)s %(levelname)s %(message)s')
    infoname = names.course_structure_file(coursename, base_path)
    ci = courseinformation.extract(infoname)
    connection = opendb()

    #learnermode.process(coursename, base_path, connection, ci)
    # learnermode.sessions(ci, base_path, connection,
    #                     bufferLocation=bufferLocation)
    # print ci

    #forummode.forum_interaction(coursename, base_path, ci, connection)
    #forummode.forum_sessions(coursename, base_path, ci, connection,
    #                         bufferLocation=bufferLocation)

    #videomode.video_interaction(coursename, base_path, ci, connection,
    #                         bufferLocation=bufferLocation)

    #quiz_mode.quiz_mode(coursename, base_path, ci, connection,
    #                         bufferLocation=bufferLocation)

    quiz_mode.quiz_session(coursename, base_path, ci, connection,
                             bufferLocation=bufferLocation)

def main(data_path, course_list_path=None):

    cursor = opendb().cursor()

    # Gather the list of translated courses
    translated_courses = set()

    if not course_list_path:
        course_list_path = os.path.join(data_path, "translated_course_list")

    print 'data_path:', data_path
    print 'course_list_path:', course_list_path
    # return

    if not os.path.isfile(course_list_path):
        course_list_file = open(course_list_path, "w")
        course_list_file.close()

    input_file = open(course_list_path, "r")
    lines = input_file.readlines()
    for line in lines:
        line = line.replace("\n", "")
        translated_courses.add(line)
    input_file.close()

    print str(len(translated_courses)) + "\tcourses have been translated."

    # Keep track of translated courses
    output_file = open(course_list_path, "a")

    # Search for the courses that have not been translated
    log_folders = os.listdir(data_path)
    for log_folder in log_folders:

        if not os.path.isdir(data_path + log_folder):
            continue

        if log_folder not in translated_courses:
            try:

                print "Start to translating course\t" + log_folder

                # zip_files & unzip_files folder path
                zip_folder_path = data_path + log_folder + "/zip_files/"
                unzip_folder_path = data_path + log_folder + "/unzip_files/"
                metadata_path = data_path + log_folder + "/metadata/"

                if not os.path.exists(unzip_folder_path):
                    os.mkdir(unzip_folder_path)

                # Uncompress the log files
                log_files = os.listdir(zip_folder_path)
                for log_file in log_files:
                    if ".gz" in log_file:
                        gz_file = gzip.GzipFile(zip_folder_path + log_file)
                        log_file = log_file.replace(".gz", "")
                        open(
                            unzip_folder_path + log_file, "w+").write(gz_file.read())
                        gz_file.close()

                # Filter out irrelevant records
                meta_files = os.listdir(metadata_path)
                for file in meta_files:
                    if "course_structure" in file:
                        course_structure_file = open(metadata_path + file, "r")
                        jsonObject = json.loads(course_structure_file.read())
                        for record in jsonObject:
                            if jsonObject[record]["category"] == "course":
                                # Course ID
                                course_id = record
                                if course_id.startswith("block-"):
                                    course_id = course_id.replace(
                                        "block-", "course-")
                                    course_id = course_id.replace(
                                        "+type@course+block@course", "")
                                if course_id.startswith("i4x://"):
                                    course_id = course_id.replace("i4x://", "")
                                    course_id = course_id.replace(
                                        "course/", "")

                filter_folder_path = data_path + "filter_folder/"
                if not os.path.exists(filter_folder_path):
                    os.mkdir(filter_folder_path)

                log_files = os.listdir(unzip_folder_path)
                for file in log_files:

                    filter_file_path = filter_folder_path + file
                    filter_file = open(filter_file_path, 'wb')

                    unfilter_file_path = unzip_folder_path + file
                    input_file = open(unfilter_file_path, "r")
                    for line in input_file:
                        jsonObject = json.loads(line)
                        if course_id in jsonObject["context"]["course_id"]:
                            filter_file.write(line)
                    filter_file.close()
                    input_file.close()

                # Remove unzip files
                # for file in log_files:
                #    os.remove(unzip_folder_path + log_file)

                # Translate the log files
                log_path = "/Volumes/NETAC/EdX/Clear-out/" + log_folder + "/"
                metadata_path = log_path

                # 1. Learner Mode
                learner_mode(metadata_path, cursor)
                sessions(metadata_path, log_path, cursor)

                print "Learner mode finished."

                # 2. Forum Mode
                forum_interaction(metadata_path, cursor)
                forum_sessions(metadata_path, log_path, cursor)

                print "Forum mode finished."

                # 3. Video Mode
                video_interaction(metadata_path, log_path, cursor)

                print "Video mode finished."

                # 4. Quiz Mode
                quiz_mode(metadata_path, log_path, cursor)
                quiz_sessions(metadata_path, log_path, cursor)

                print "Quiz mode finished."

                # 5. Survey Mode
                # pre_id_index = 13
                # post_id_index = 10
                # survey_mode(metadata_path, survey_path, cursor, pre_id_index, post_id_index)

                print "Survey mode finished."

                log_files = os.listdir(log_path)
                for log_file in log_files:
                    os.remove(log_path + log_file)

                # Record translated course
                output_file.write(log_folder + "\n")

            except Exception as e:

                print "Error occurs when translating\t" + log_folder
                print e

    output_file.close()


###############################################################################
if __name__ == '__main__':

    #     parser = argparse.ArgumentParser(description='Process DelftX datafiles')
    #     parser.add_argument('--data_path', type=str, default="")
    #     parser.add_argument(
    #         '--translated_course_list', type=str,
    #         help='location of file holding list of processed courses')
    #     args = parser.parse_args()
    #     main(args.data_path, course_list_path=args.translated_course_list)

    parser = argparse.ArgumentParser(description='Process DelftX datafiles')
    parser.add_argument('coursename', type=str, default="")
    parser.add_argument('--directory', type=str, default="")
    parser.add_argument('--bufferlocation', type=str, default=None)
    args = parser.parse_args()
    processcourse(args.coursename, args.directory,
                  bufferLocation=args.bufferlocation)
    print "All finished."
