import json
from time import strftime, gmtime
import datetime
import logging
from dateutil.parser import parse
import operator

import delftx.util
from delftx.util import names, BaseEventProcessor

logger = logging.getLogger(__name__)


def forum_interaction(coursename, base_path, course_metadata_map, connection):

    forum_interaction_records = []

    # Processing forum data

    mfile = names.mongo_file(coursename, base_path)
    forum_file = open(mfile, "r")
    logger.info("Start processing mongo file [%s]" % (mfile,))
    for line in forum_file:
        jsonObject = json.loads(line)

        post_id = jsonObject["_id"]["$oid"]
        course_learner_id = jsonObject[
            "course_id"] + "_" + jsonObject["author_id"]

        post_type = jsonObject["_type"]
        if post_type == "CommentThread":
            post_type += "_" + jsonObject["thread_type"]
        if "parent_id" in jsonObject and jsonObject["parent_id"] != "":
            post_type = "Comment_Reply"

        post_title = ""
        if "title" in jsonObject:
            post_title = jsonObject["title"]

        post_content = jsonObject["body"]

        post_timestamp = jsonObject["created_at"]["$date"]
        if isinstance(post_timestamp, int):
            post_timestamp = strftime(
                "%Y-%m-%d %H:%M:%S", gmtime(post_timestamp / 1000))
            post_timestamp = datetime.datetime.strptime(
                post_timestamp, "%Y-%m-%d %H:%M:%S")
        if isinstance(post_timestamp, unicode):
            post_timestamp = post_timestamp[0:19]
            post_timestamp = post_timestamp.replace("T", " ")
            post_timestamp = datetime.datetime.strptime(
                post_timestamp, "%Y-%m-%d %H:%M:%S")

        post_parent_id = ""
        if "parent_id" in jsonObject:
            post_parent_id = jsonObject["parent_id"]["$oid"]

        post_thread_id = ""
        if "comment_thread_id" in jsonObject:
            post_thread_id = jsonObject["comment_thread_id"]["$oid"]

        post_title = post_title.replace("\n", " ")
        post_title = post_title.replace("\\", "\\\\")
        post_title = post_title.replace("\'", "\\'")

        post_content = post_content.replace("\n", " ")
        post_content = post_content.replace("\\", "\\\\")
        post_content = post_content.replace("\'", "\\'")

        if post_timestamp < course_metadata_map["end_time"]:

            array = [post_id, course_learner_id, post_type, post_title,
                     post_content, post_timestamp, post_parent_id,
                     post_thread_id]
            forum_interaction_records.append(array)

    forum_file.close()
    logger.info("Finished processing mongo file")

    # Database version
    cursor = connection.cursor(prepared=True)

    logger.debug("Starting logging %s forum interaction records to database" %
                 (len(forum_interaction_records)))
    sql = ("insert into "
           "forum_interaction(post_id, course_learner_id, post_type, "
           "                  post_title, post_content, post_timestamp, "
           "                  post_parent_id, post_thread_id) "
           "values (%s,%s,%s,%s,%s,%s,%s,%s)")

    for array in forum_interaction_records:
        post_id = array[0]
        course_learner_id = array[1]
        post_type = array[2]
        post_title = array[3]
        post_content = array[4]
        post_timestamp = array[5]
        post_parent_id = array[6]
        post_thread_id = array[7]
        cursor.execute(sql, (post_id, course_learner_id,
                             post_type, post_title, post_content,
                             post_timestamp, post_parent_id, post_thread_id))

    logger.debug("Finished logging %s forum interaction records to database" %
                 (len(forum_interaction_records)))


def forum_sessions(coursename, base_path, course_metadata_map, connection,
                   bufferLocation=None):
    oneday = datetime.timedelta(days=1)

    current_date = parse(course_metadata_map["start_date"]).date()
    end_next_date = parse(course_metadata_map["end_date"]).date() + oneday

    forum_event_types = []
    forum_event_types.append("edx.forum.comment.created")
    forum_event_types.append("edx.forum.response.created")
    forum_event_types.append("edx.forum.response.voted")
    forum_event_types.append("edx.forum.thread.created")
    forum_event_types.append("edx.forum.thread.voted")
    forum_event_types.append("edx.forum.searched")

    learner_all_event_logs = {}
    updated_learner_all_event_logs = {}

    forum_sessions_record = []

    logger.info('Scanning event logfiles from [%s] until [%s]' %
                (current_date, end_next_date))

    while current_date < end_next_date:

        logger.debug("Opening logfile")
        input_file = delftx.util.openeventlogfile(current_date, base_path,
                                                  bufferLocation)
        logger.debug("Opened logfile")
        lines = input_file.readlines()
        logger.debug("Read all input into memory. Processing %s lines",
                     (len(lines)))
        learner_all_event_logs.clear()
        learner_all_event_logs = updated_learner_all_event_logs.copy()
        updated_learner_all_event_logs.clear()

        # Course_learner_id set
        course_learner_id_set = set()
        for course_learner_id in learner_all_event_logs.keys():
            course_learner_id_set.add(course_learner_id)

        for line in lines:
            jsonObject = json.loads(line)

            # For forum session separation
            global_learner_id = jsonObject["context"]["user_id"]
            event_type = str(jsonObject["event_type"])

            if "/discussion/" in event_type or event_type in forum_event_types:
                if event_type != "edx.forum.searched":
                    event_type = "forum_activity"

            if global_learner_id != "":
                course_id = jsonObject["context"]["course_id"]
                course_learner_id = course_id + "_" + str(global_learner_id)

                event_time = jsonObject["time"]
                event_time = event_time[0:19]
                event_time = event_time.replace("T", " ")
                event_time = datetime.datetime.strptime(
                    event_time, "%Y-%m-%d %H:%M:%S")

                if course_learner_id in course_learner_id_set:
                    learner_all_event_logs[course_learner_id].append(
                        {"event_time": event_time, "event_type": event_type})
                else:
                    learner_all_event_logs[course_learner_id] = [
                        {"event_time": event_time, "event_type": event_type}]
                    course_learner_id_set.add(course_learner_id)
        logger.debug("%s json-lines processed" % (len(lines,)))
        logger.debug("Processing %s learner ids" %
                     (len(learner_all_event_logs.keys())))
        # For forum session separation
        for learner in learner_all_event_logs.keys():
            course_learner_id = learner
            event_logs = learner_all_event_logs[learner]

            # Sorting
            event_logs.sort(key=operator.itemgetter('event_time'))

            session_id = ""
            start_time = ""
            end_time = ""
            times_search = 0
            final_time = ""
            for i in range(len(event_logs)):
                if session_id == "":
                    if (event_logs[i]["event_type"] in
                            ["forum_activity", "edx.forum.searched"]):
                        # Initialization
                        session_id = "forum_session_" + course_learner_id
                        start_time = event_logs[i]["event_time"]
                        end_time = event_logs[i]["event_time"]
                        if event_logs[i]["event_type"] == "edx.forum.searched":
                            times_search += 1
                else:
                    if (event_logs[i]["event_type"] in
                            ["forum_activity", "edx.forum.searched"]):
                        if (event_logs[i]["event_time"] >
                                end_time + datetime.timedelta(hours=0.5)):
                            session_id = session_id + "_" + \
                                str(start_time) + "_" + str(end_time)
                            duration = ((end_time - start_time).days *
                                        24 * 60 * 60 +
                                        (end_time - start_time).seconds)
                            if duration > 5:
                                array = [
                                    session_id, course_learner_id,
                                    times_search, start_time, end_time,
                                    duration]
                                forum_sessions_record.append(array)

                            final_time = event_logs[i]["event_time"]

                            # Re-initialization
                            session_id = "forum_session_" + course_learner_id
                            start_time = event_logs[i]["event_time"]
                            end_time = event_logs[i]["event_time"]
                            if (event_logs[i]["event_type"] ==
                                    "edx.forum.searched"):
                                times_search = 1
                        else:
                            end_time = event_logs[i]["event_time"]
                            if (event_logs[i]["event_type"] ==
                                    "edx.forum.searched"):
                                times_search += 1
                    else:
                        end_time = event_logs[i]["event_time"]
                        session_id = session_id + "_" + \
                            str(start_time) + "_" + str(end_time)
                        duration = (end_time - start_time).days * \
                            24 * 60 * 60 + (end_time - start_time).seconds
                        if duration > 5:
                            array = [
                                session_id, course_learner_id, times_search,
                                start_time, end_time, duration]
                            forum_sessions_record.append(array)
                        final_time = event_logs[i]["event_time"]

                        # Re-initialization
                        session_id = ""
                        start_time = ""
                        end_time = ""
                        times_search = 0
            if final_time != "":
                new_logs = []
                for log in event_logs:
                    if log["event_time"] >= final_time:
                        new_logs.append(log)

                updated_learner_all_event_logs[course_learner_id] = new_logs
        input_file.close()
        logger.debug("Processed learner id's")
        current_date = current_date + oneday

    # Database version
    cursor = connection.cursor(prepared=True)
    sql = ("insert into "
           "forum_sessions (session_id, course_learner_id, times_search, "
           "                start_time, end_time, duration) "
           "values (%s,%s,%s,%s,%s,%s)")

    logger.debug("Starting logging %s forum_session entries to database" %
                 (len(forum_sessions_record)))
    for array in forum_sessions_record:
        session_id = array[0]
        course_learner_id = array[1]
        times_search = array[2]
        start_time = array[3]
        end_time = array[4]
        duration = array[5]
        cursor.execute(sql, (session_id, course_learner_id, times_search,
                             start_time, end_time, duration))
    logger.debug("Finished logging %s forum_session entries to "
                 "database" % (len(forum_sessions_record),))


class Sessions(BaseEventProcessor):

    def __init__(self, course_metadata_map, base_path, connection):
        super(Sessions,
              self).__init__(course_metadata_map, base_path, connection)

        self.learner_all_event_logs = {}
        self.updated_learner_all_event_logs = {}
        self.forum_sessions_record = []

    forum_event_types = []
    forum_event_types.append("edx.forum.comment.created")
    forum_event_types.append("edx.forum.response.created")
    forum_event_types.append("edx.forum.response.voted")
    forum_event_types.append("edx.forum.thread.created")
    forum_event_types.append("edx.forum.thread.voted")
    forum_event_types.append("edx.forum.searched")

    def init_next_file(self):
        self.learner_all_event_logs.clear()
        self.learner_all_event_logs = self.updated_learner_all_event_logs.copy()
        self.updated_learner_all_event_logs.clear()

        # Course_learner_id set
        self.course_learner_id_set = set()
        for course_learner_id in self.learner_all_event_logs.keys():
            self.course_learner_id_set.add(course_learner_id)

    def post_next_file(self):
        logger.debug("Processing %s learner ids" %
                     (len(self.learner_all_event_logs.keys())))
        # For forum session separation
        for learner in self.learner_all_event_logs.keys():
            course_learner_id = learner
            event_logs = self.learner_all_event_logs[learner]

            # Sorting
            event_logs.sort(key=operator.itemgetter('event_time'))

            session_id = ""
            start_time = ""
            end_time = ""
            times_search = 0
            final_time = ""
            for i in range(len(event_logs)):
                if session_id == "":
                    if (event_logs[i]["event_type"] in
                            ["forum_activity", "edx.forum.searched"]):
                        # Initialization
                        session_id = "forum_session_" + course_learner_id
                        start_time = event_logs[i]["event_time"]
                        end_time = event_logs[i]["event_time"]
                        if event_logs[i]["event_type"] == "edx.forum.searched":
                            times_search += 1
                else:
                    if (event_logs[i]["event_type"] in
                            ["forum_activity", "edx.forum.searched"]):
                        if (event_logs[i]["event_time"] >
                                end_time + datetime.timedelta(hours=0.5)):
                            session_id = session_id + "_" + \
                                str(start_time) + "_" + str(end_time)
                            duration = ((end_time - start_time).days *
                                        24 * 60 * 60 +
                                        (end_time - start_time).seconds)
                            if duration > 5:
                                array = [
                                    session_id, course_learner_id,
                                    times_search, start_time, end_time,
                                    duration]
                                self.forum_sessions_record.append(array)

                            final_time = event_logs[i]["event_time"]

                            # Re-initialization
                            session_id = "forum_session_" + course_learner_id
                            start_time = event_logs[i]["event_time"]
                            end_time = event_logs[i]["event_time"]
                            if (event_logs[i]["event_type"] ==
                                    "edx.forum.searched"):
                                times_search = 1
                        else:
                            end_time = event_logs[i]["event_time"]
                            if (event_logs[i]["event_type"] ==
                                    "edx.forum.searched"):
                                times_search += 1
                    else:
                        end_time = event_logs[i]["event_time"]
                        session_id = session_id + "_" + \
                            str(start_time) + "_" + str(end_time)
                        duration = (end_time - start_time).days * \
                            24 * 60 * 60 + (end_time - start_time).seconds
                        if duration > 5:
                            array = [
                                session_id, course_learner_id, times_search,
                                start_time, end_time, duration]
                            self.forum_sessions_record.append(array)
                        final_time = event_logs[i]["event_time"]

                        # Re-initialization
                        session_id = ""
                        start_time = ""
                        end_time = ""
                        times_search = 0
            if final_time != "":
                new_logs = []
                for log in event_logs:
                    if log["event_time"] >= final_time:
                        new_logs.append(log)

                self.updated_learner_all_event_logs[
                    course_learner_id] = new_logs
        logger.debug("Done processing %s learner ids" %
                     (len(self.learner_all_event_logs.keys())))

    def handleEvent(self, jsonObject):
        # For forum session separation
        global_learner_id = jsonObject["context"]["user_id"]
        event_type = str(jsonObject["event_type"])

        if ("/discussion/" in event_type or
                event_type in self.forum_event_types):
            if event_type != "edx.forum.searched":
                event_type = "forum_activity"

        if global_learner_id != "":
            course_id = jsonObject["context"]["course_id"]
            course_learner_id = course_id + "_" + str(global_learner_id)

            event_time = jsonObject["time"]
            event_time = event_time[0:19]
            event_time = event_time.replace("T", " ")
            event_time = datetime.datetime.strptime(
                event_time, "%Y-%m-%d %H:%M:%S")

            if course_learner_id in self.course_learner_id_set:
                self.learner_all_event_logs[course_learner_id].append(
                    {"event_time": event_time, "event_type": event_type})
            else:
                self.learner_all_event_logs[course_learner_id] = [
                    {"event_time": event_time, "event_type": event_type}]
                self.course_learner_id_set.add(course_learner_id)

    def postprocessing(self):
        # Database version
        cursor = self.connection.cursor(prepared=True)
        sql = ("insert into "
               "forum_sessions (session_id, course_learner_id, times_search, "
               "                start_time, end_time, duration) "
               "values (%s,%s,%s,%s,%s,%s)")

        logger.debug("Starting logging %s forum_session entries to database" %
                     (len(self.forum_sessions_record)))
        for array in self.forum_sessions_record:
            session_id = array[0]
            course_learner_id = array[1]
            times_search = array[2]
            start_time = array[3]
            end_time = array[4]
            duration = array[5]
            cursor.execute(sql, (session_id, course_learner_id, times_search,
                                 start_time, end_time, duration))
        logger.debug("Finished logging %s forum_session entries to "
                     "database" % (len(self.forum_sessions_record),))
