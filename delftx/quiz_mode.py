import datetime
from dateutil.parser import parse
import logging
import json
import operator
import delftx.util

logger = logging.getLogger(__name__)


def quiz_mode(coursename, base_path, course_metadata_map, connection,
              bufferLocation=None):

    oneday = datetime.timedelta(days=1)

    current_date = parse(course_metadata_map["start_date"]).date()
    end_next_date = parse(course_metadata_map["end_date"]).date() + oneday

    quiz_question_record = []
    submissions = {}
    assessments = {}

    quiz_question_array = course_metadata_map["quiz_question_array"]
    block_type_map = course_metadata_map["block_type_map"]
    for question_id in quiz_question_array:
        quiz_question_parent = (
            course_metadata_map["child_parent_map"][question_id])
        while quiz_question_parent not in block_type_map:
            quiz_question_parent = (
                course_metadata_map["child_parent_map"][quiz_question_parent])
        quiz_question_type = block_type_map[quiz_question_parent]
        array = [question_id, quiz_question_type]
        quiz_question_record.append(array)

    # Processing events data
    submission_event_collection = []

    # Problem check
    submission_event_collection.append("problem_check")     # Server
    submission_event_collection.append("save_problem_check")
    submission_event_collection.append("problem_check_fail")
    submission_event_collection.append("save_problem_check_fail")

    # The server emits a problem_graded event each time a user selects Check
    # for a problem and it is graded success- fully.
    submission_event_collection.append("problem_graded")

    # The server emits problem_rescore events when a problem is successfully
    # rescored.
    submission_event_collection.append("problem_rescore")
    submission_event_collection.append("problem_rescore_fail")

    submission_event_collection.append("problem_reset")  # event_source: serve
    submission_event_collection.append("reset_problem")
    submission_event_collection.append("reset_problem_fail")

    # The server emits problem_save events after a user saves a problem.
    submission_event_collection.append("problem_save")  # event_source: server
    submission_event_collection.append("save_problem_fail")
    submission_event_collection.append("save_problem_success")

    # Show answer
    submission_event_collection.append("problem_show")
    submission_event_collection.append("showanswer")

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

        for line in lines:
            jsonObject = json.loads(line)

            if jsonObject["event_type"] in submission_event_collection:

                global_learner_id = jsonObject["context"]["user_id"]

                if global_learner_id != "":

                    course_id = jsonObject["context"]["course_id"]
                    course_learner_id = course_id + \
                        "_" + str(global_learner_id)

                    question_id = ""

                    grade = ""
                    max_grade = ""

                    event_time = jsonObject["time"]
                    event_time = event_time[0:19]
                    event_time = event_time.replace("T", " ")
                    event_time = datetime.datetime.strptime(
                        event_time, "%Y-%m-%d %H:%M:%S")

                    if isinstance(jsonObject["event"], dict):
                        question_id = jsonObject["event"]["problem_id"]

                        # The fields "grade" and "max_grade" are specific to
                        # submission event "problem_check"
                        if ("grade" in jsonObject["event"] and
                                "max_grade" in jsonObject["event"]):
                            grade = jsonObject["event"]["grade"]
                            max_grade = jsonObject["event"]["max_grade"]

                    if question_id != "":

                        submission_id = course_learner_id + "_" + question_id

                        # For submissions
                        array = [
                            submission_id, course_learner_id, question_id,
                            event_time]
                        submissions[submission_id] = array

                        # For assessments
                        if grade != "" and max_grade != "":
                            array = [
                                submission_id, course_learner_id, max_grade,
                                grade]
                            assessments[submission_id] = array

        logger.debug("%s json-lines processed" % (len(lines,)))
        current_date = current_date + oneday

    submission_record = []
    assessment_record = []

    for submission_id in submissions.keys():
        submission_record.append(submissions[submission_id])

    for assessment_id in assessments.keys():
        assessment_record.append(assessments[assessment_id])

    # Database version
    cursor = connection.cursor(prepared=True)
    sql = ("insert into quiz_questions(question_id, question_type) "
           "values (%s,%s)")

    logger.debug("Starting logging %s quiz_questions entries to database" %
                 (len(quiz_question_record)))
    # Quiz_question table
    for array in quiz_question_record:
        question_id = array[0]
        question_type = array[1]
        cursor.execute(sql, (question_id, question_type))
    logger.debug("Finished logging %s quiz_questions entries to database" %
                 (len(quiz_question_record)))

    # Submissions table
    sql = ("insert into "
           "submissions(submission_id, course_learner_id, question_id, "
           "submission_timestamp) values (%s,%s,%s,%s)")
    logger.debug("Starting logging %s submission entries to database" %
                 (len(submission_record)))

    for array in submission_record:
        submission_id = array[0]
        course_learner_id = array[1]
        question_id = array[2]
        submission_timestamp = array[3]
        cursor.execute(sql,
                       (submission_id, course_learner_id, question_id,
                        event_time))  # This should probably not be event_time!
    logger.debug("Finished logging %s submission entries to database" %
                 (len(submission_record)))

    # Submissions table
    sql = ("insert into "
           "assessments(assessment_id, course_learner_id, max_grade, grade) "
           "values (%s,%s,%s,%s)")

    logger.debug("Starting logging %s assessment entries to database" %
                 (len(assessment_record)))
    for array in assessment_record:
        assessment_id = array[0]
        course_learner_id = array[1]
        max_grade = array[2]
        grade = array[3]
        cursor.execute(sql,
                       (assessment_id, course_learner_id, max_grade, grade))
    logger.debug("Finished logging %s assessment entries to database" %
                 (len(assessment_record)))


def quiz_session(coursename, base_path, course_metadata_map, connection,
                 bufferLocation=None):

    oneday = datetime.timedelta(days=1)

    current_date = parse(course_metadata_map["start_date"]).date()
    end_next_date = parse(course_metadata_map["end_date"]).date() + oneday

    # Processing events data
    submission_event_collection = []

    # Problem check
    submission_event_collection.append("problem_check")     # Server
    submission_event_collection.append("save_problem_check")
    submission_event_collection.append("problem_check_fail")
    submission_event_collection.append("save_problem_check_fail")

    # The server emits a problem_graded event each time a user selects Check
    # for a problem and it is graded success- fully.
    submission_event_collection.append("problem_graded")

    # The server emits problem_rescore events when a problem is successfully
    # rescored.
    submission_event_collection.append("problem_rescore")
    submission_event_collection.append("problem_rescore_fail")

    submission_event_collection.append("problem_reset")  # event_source: serve
    submission_event_collection.append("reset_problem")
    submission_event_collection.append("reset_problem_fail")

    # The server emits problem_save events after a user saves a problem.
    submission_event_collection.append("problem_save")  # event_source: server
    submission_event_collection.append("save_problem_fail")
    submission_event_collection.append("save_problem_success")

    # Show answer
    submission_event_collection.append("problem_show")
    submission_event_collection.append("showanswer")

    child_parent_map = course_metadata_map["child_parent_map"]

    learner_all_event_logs = {}
    updated_learner_all_event_logs = {}
    quiz_sessions = {}

    while current_date < end_next_date:
        learner_all_event_logs.clear()
        learner_all_event_logs = updated_learner_all_event_logs.copy()
        updated_learner_all_event_logs.clear()

        # Course_learner_id set
        course_learner_id_set = set()
        for course_learner_id in learner_all_event_logs.keys():
            course_learner_id_set.add(course_learner_id)

        logger.debug("Opening logfile")
        input_file = delftx.util.openeventlogfile(current_date, base_path,
                                                  bufferLocation)
        logger.debug("Opened logfile")
        lines = input_file.readlines()
        logger.debug("Read all input into memory. Processing %s lines",
                     (len(lines)))

        for line in lines:
            jsonObject = json.loads(line)

            global_learner_id = jsonObject["context"]["user_id"]
            event_type = str(jsonObject["event_type"])

            if global_learner_id != "":

                course_id = jsonObject["context"]["course_id"]
                course_learner_id = course_id + \
                    "_" + str(global_learner_id)

                event_time = jsonObject["time"]
                event_time = event_time[0:19]
                event_time = event_time.replace("T", " ")
                event_time = datetime.datetime.strptime(
                    event_time, "%Y-%m-%d %H:%M:%S")

                if course_learner_id in learner_all_event_logs:
                    learner_all_event_logs[course_learner_id].append(
                        {"event_time": event_time, "event_type": event_type})
                else:
                    learner_all_event_logs[course_learner_id] = [
                        {"event_time": event_time, "event_type": event_type}]

        # For quiz session separation
        for course_learner_id in learner_all_event_logs.keys():

            event_logs = learner_all_event_logs[course_learner_id]

            # Sorting
            event_logs.sort(key=operator.itemgetter('event_time'))

            session_id = ""
            start_time = ""
            end_time = ""

            final_time = ""

            for i in range(len(event_logs)):

                if session_id == "":

                    if ("problem+block" in event_logs[i]["event_type"] or
                            "_problem;_" in event_logs[i]["event_type"]):
                        event_type_array = event_logs[
                            i]["event_type"].split("/")

                        if "problem+block" in event_logs[i]["event_type"]:
                            question_id = event_type_array[4]

                        if "_problem;_" in event_logs[i]["event_type"]:
                            question_id = event_type_array[
                                6].replace(";_", "/")

                        if question_id in child_parent_map.keys():
                            parent_block_id = child_parent_map[question_id]
                            session_id = "quiz_session_" + \
                                parent_block_id + "_" + \
                                course_learner_id
                            start_time = event_logs[i]["event_time"]
                            end_time = event_logs[i]["event_time"]
                else:
                    if ("problem+block" in event_logs[i]["event_type"] or
                            "_problem;_" in event_logs[i]["event_type"] or
                            event_logs[i]["event_type"] in
                            submission_event_collection):

                        if (event_logs[i]["event_time"] >
                                end_time + datetime.timedelta(hours=0.5)):

                            if session_id in quiz_sessions:
                                quiz_sessions[session_id]["time_array"].append(
                                    {"start_time": start_time,
                                     "end_time": end_time})
                            else:
                                quiz_sessions[session_id] = {
                                    "course_learner_id": course_learner_id,
                                    "time_array": [
                                        {"start_time": start_time,
                                         "end_time": end_time}]}

                            final_time = event_logs[i]["event_time"]

                            if ("problem+block" in
                                event_logs[i]["event_type"] or
                                "_problem;_" in
                                    event_logs[i]["event_type"]):
                                event_type_array = event_logs[
                                    i]["event_type"].split("/")
                                question_id = event_type_array[4]

                                if question_id in child_parent_map.keys():
                                    parent_block_id = child_parent_map[
                                        question_id]
                                    session_id = "quiz_session_" + \
                                        parent_block_id + "_" + \
                                        course_learner_id
                                    start_time = event_logs[
                                        i]["event_time"]
                                    end_time = event_logs[
                                        i]["event_time"]
                                else:
                                    session_id = ""
                                    start_time = ""
                                    end_time = ""
                        else:
                            end_time = event_logs[i]["event_time"]

                    else:

                        end_time = event_logs[i]["event_time"]

                        if session_id in quiz_sessions:
                            quiz_sessions[session_id]["time_array"].append(
                                {"start_time": start_time,
                                 "end_time": end_time})
                        else:
                            quiz_sessions[session_id] = {
                                "course_learner_id": course_learner_id,
                                "time_array": [
                                    {"start_time": start_time,
                                     "end_time": end_time}]}

                        final_time = event_logs[i]["event_time"]

                        session_id = ""
                        start_time = ""
                        end_time = ""

            if final_time != "":
                new_logs = []
                for log in event_logs:
                    if log["event_time"] >= final_time:
                        new_logs.append(log)

                updated_learner_all_event_logs[
                    course_learner_id] = new_logs

        logger.debug("%s json-lines processed" % (len(lines,)))
        current_date = current_date + oneday

    logger.debug('Processing %s quiz session events' %
                 (len(quiz_sessions.keys())))
    # To compress the session event_logs
    for session_id in quiz_sessions.keys():
        if len(quiz_sessions[session_id]["time_array"]) > 1:
            start_time = ""
            end_time = ""
            updated_time_array = []

            for i in range(len(quiz_sessions[session_id]["time_array"])):
                if i == 0:
                    start_time = quiz_sessions[session_id][
                        "time_array"][i]["start_time"]
                    end_time = quiz_sessions[session_id][
                        "time_array"][i]["end_time"]
                else:
                    if quiz_sessions[session_id]["time_array"][i]["start_time"] > end_time + datetime.timedelta(hours=0.5):
                        updated_time_array.append(
                            {"start_time": start_time, "end_time": end_time})
                        start_time = quiz_sessions[session_id][
                            "time_array"][i]["start_time"]
                        end_time = quiz_sessions[session_id][
                            "time_array"][i]["end_time"]
                    else:
                        end_time = quiz_sessions[session_id][
                            "time_array"][i]["end_time"]

                        if (i == len(quiz_sessions[session_id]["time_array"]) -
                                1):
                            updated_time_array.append(
                                {"start_time": start_time,
                                 "end_time": end_time})

            quiz_sessions[session_id]["time_array"] = updated_time_array

    quiz_session_record = []

    logger.debug('Re-processing %s quiz session events' %
                 (len(quiz_sessions.keys())))
    for session_id in quiz_sessions.keys():
        course_learner_id = quiz_sessions[session_id]["course_learner_id"]
        for i in range(len(quiz_sessions[session_id]["time_array"])):

            start_time = quiz_sessions[session_id][
                "time_array"][i]["start_time"]
            end_time = quiz_sessions[session_id]["time_array"][i]["end_time"]
            if start_time < end_time:
                duration = (end_time - start_time).days * 24 * \
                    60 * 60 + (end_time - start_time).seconds
                final_session_id = session_id + "_" + \
                    str(start_time) + "_" + str(end_time)

                if duration > 5:
                    array = [
                        final_session_id, course_learner_id, start_time,
                        end_time, duration]
                    quiz_session_record.append(array)

    # Database version
    cursor = connection.cursor(prepared=True)
    sql = ("insert into "
           "quiz_sessions (session_id, course_learner_id, start_time, "
           "               end_time, duration) values (%s,%s,%s,%s,%s)")
    logger.debug("Starting logging %s quiz_session entries to database" %
                 (len(quiz_session_record)))

    for array in quiz_session_record:
        session_id = array[0]
        course_learner_id = array[1]
        start_time = array[2]
        end_time = array[3]
        duration = array[4]
        cursor.execute(sql,
                       (session_id, course_learner_id, start_time, end_time,
                        duration))

    logger.debug("Finished logging %s quiz_session entries to database" %
                 (len(quiz_session_record)))
