import json
from time import strftime, gmtime
import datetime
import logging

from delftx.util import names

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
