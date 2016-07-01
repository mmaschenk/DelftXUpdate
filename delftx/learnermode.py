from delftx.util import names, BaseEventProcessor
import delftx.util
import datetime
import json
import operator
from dateutil.parser import parse
import logging
from delftx.util import eventGenerator

logger = logging.getLogger(__name__)


def process(course_name, base_path, connection, course_metadata_map):

    course_record = []
    course_element_record = []
    learner_index_record = []
    course_learner_record = []
    learner_demographic_record = []

    # Collect course information
    # course_metadata_map = ExtractCourseInformation(metadata_path)
    course_record.append(
        [course_metadata_map["course_id"],
         course_metadata_map["course_name"],
         course_metadata_map["start_time"],
         course_metadata_map["end_time"]])

    # Course_element table
    for element_id in course_metadata_map["element_time_map"].keys():

        element_start_time = course_metadata_map[
            "element_time_map"][element_id]
        week = ((element_start_time - course_metadata_map["start_time"]).days /
                7 + 1)

        array = [element_id, course_metadata_map["element_type_map"]
                 [element_id], week, course_metadata_map["course_id"]]
        course_element_record.append(array)

    # Learner_demographic table
    learner_mail_map = {}

    # Course_learner table
    course_learner_map = {}

    # Enrolled learners set
    enrolled_learner_set = set()

    course_id = ""

    # Processing student_courseenrollment data
    input_file = open(
        names.course_enrollment_file(course_name, base_path), "r")
    # open(metadata_path + file, "r")
    input_file.readline()
    lines = input_file.readlines()

    for line in lines:
        record = line.split("\t")
        global_learner_id = record[1]
        course_id = record[2]
        time = datetime.datetime.strptime(
            record[3], "%Y-%m-%d %H:%M:%S")
        course_learner_id = course_id + "_" + global_learner_id

        if course_metadata_map["end_time"] != time:
            enrolled_learner_set.add(global_learner_id)

            array = [global_learner_id, course_id, course_learner_id]
            learner_index_record.append(array)

            course_learner_map[global_learner_id] = course_learner_id

    input_file.close()

    logger.info("The number of enrolled learners is: [%s]" %
                (len(enrolled_learner_set),))

    # Processing auth_user data
    input_file = open(names.auth_user_file(course_name, base_path), "r")
    input_file.readline()
    lines = input_file.readlines()
    for line in lines:
        record = line.split("\t")
        if record[0] in enrolled_learner_set:
            learner_mail_map[record[0]] = record[4]
    input_file.close()

    # Processing certificates_generatedcertificate data
    num_uncertifiedLearners = 0
    num_certifiedLearners = 0
    input_file = open(
        names.certificates_generatedcertificate_file(
            course_name, base_path), "r")
    input_file.readline()
    lines = input_file.readlines()

    for line in lines:
        record = line.split("\t")
        global_learner_id = record[1]
        final_grade = record[3]
        enrollment_mode = record[14].replace("\n", "")
        certificate_status = record[7]

        if global_learner_id in course_learner_map:
            num_certifiedLearners += 1
            array = [course_learner_map[global_learner_id],
                     final_grade, enrollment_mode, certificate_status]
            course_learner_record.append(array)
        else:
            num_uncertifiedLearners += 1

    input_file.close()

    logger.info("The number certified learners is: %s" %
                (num_uncertifiedLearners,))
    logger.info("The number of uncertified learners is: %s" %
                (num_certifiedLearners,))

    # Processing auth_userprofile data
    input_file = open(names.auth_userprofile_file(course_name, base_path), "r")
    input_file.readline()
    lines = input_file.readlines()

    for line in lines:
        record = line.split("\t")
        global_learner_id = record[1]
        gender = record[7]
        year_of_birth = record[9]
        level_of_education = record[10]
        country = record[13]

        course_learner_id = course_id + "_" + global_learner_id

        if global_learner_id in enrolled_learner_set:
            array = [course_learner_id, gender, year_of_birth,
                     level_of_education, country,
                     learner_mail_map[global_learner_id]]
            learner_demographic_record.append(array)

    input_file.close()

    cursor = connection.cursor(prepared=True)
    # Database version
    # Course table
    logger.debug("Starting logging %s course_record entries to database" %
                 (len(course_record),))
    sql = ("insert into "
           "courses(course_id, course_name, start_time,end_time) "
           "values (%s,%s,%s,%s)")
    for array in course_record:
        course_id = course_metadata_map["course_id"]
        course_name = course_metadata_map["course_name"]
        start_time = course_metadata_map["start_time"]
        end_time = course_metadata_map["end_time"]
        cursor.execute(sql, (course_id, course_name, start_time, end_time))
    logger.debug("Finished logging %s course_record entries to database" %
                 (len(course_record),))

    logger.debug("Starting logging %s course_element entries to database" %
                 (len(course_element_record),))
    sql = ("insert into course_elements(element_id, element_type, "
           "                            week, course_id) "
           " values (%s,%s,%s,%s)")
    for array in course_element_record:
        element_id = array[0]
        element_type = array[1]
        week = array[2]
        course_id = array[3]
        cursor.execute(sql, (element_id, element_type, week, course_id))
    logger.debug("Finished logging %s course_element entries to database" %
                 (len(course_element_record),))
    # Learner_index table
    logger.debug("Starting logging %s learner_index entries to database" %
                 (len(learner_index_record),))

    sql = ("insert into learner_index(global_learner_id, course_id, "
           "                          course_learner_id) "
           "values (%s,%s,%s)")

    for array in learner_index_record:
        global_learner_id = array[0]
        course_id = array[1]
        course_learner_id = array[2]
        cursor.execute(sql, (global_learner_id, course_id, course_learner_id))
    logger.debug("Finished logging %s learner_index entries to database" %
                 (len(learner_index_record),))
    # Course_learner table
    logger.debug("Starting logging %s course_learner entries to database" %
                 (len(course_learner_record),))
    sql = ("insert into "
           "course_learner(course_learner_id, final_grade, enrollment_mode, "
           "               certificate_status) values (%s,%s,%s,%s)")
    for array in course_learner_record:
        course_learner_id = array[0]
        final_grade = array[1]
        enrollment_mode = array[2]
        certificate_status = array[3]

        cursor.execute(sql, (course_learner_id,
                             final_grade, enrollment_mode, certificate_status))
    logger.debug("Finished logging %s course_learner entries to database" %
                 (len(course_learner_record),))

    # Learner_demographic table
    logger.debug("Starting logging %s learner_demographic entries to "
                 "database" % (len(learner_demographic_record),))
    sql = ("insert into "
           "learner_demographic(course_learner_id, gender, year_of_birth,"
           "                    level_of_education, country, email) "
           "values (%s,%s,%s,%s,%s,%s)")

    for array in learner_demographic_record:
        course_learner_id = array[0]
        gender = array[1]
        year_of_birth = array[2]
        level_of_education = array[3]
        country = array[4]
        email = array[5]
        email = email.replace("\'", "")
        cursor.execute(sql, (course_learner_id, gender, year_of_birth,
                             level_of_education, country, email))
    logger.debug("Finished logging %s learner_demographic entries to "
                 "database" % (len(learner_demographic_record),))


class Sessions(BaseEventProcessor):

    def __init__(self, course_metadata_map, base_path, connection):
        super(Sessions,
              self).__init__(course_metadata_map, base_path, connection)

        self.learner_all_event_logs = {}
        self.updated_learner_all_event_logs = {}
        self.session_record = []

    def init_next_file(self):
        self.learner_all_event_logs.clear()
        self.learner_all_event_logs = \
            self.updated_learner_all_event_logs.copy()
        self.updated_learner_all_event_logs.clear()

        # Course_learner_id set
        self.course_learner_id_set = set()
        for course_learner_id in self.learner_all_event_logs.keys():
            self.course_learner_id_set.add(course_learner_id)

    def post_next_file(self):
        logger.debug("Processing %s learner ids" %
                     (len(self.learner_all_event_logs.keys())))

        for course_learner_id in self.learner_all_event_logs.keys():
            event_logs = self.learner_all_event_logs[course_learner_id]

            # Sorting
            event_logs.sort(key=operator.itemgetter('event_time'))

            session_id = ""
            start_time = ""
            end_time = ""

            final_time = ""

            for i in range(len(event_logs)):
                if start_time == "":
                    # Initialization
                    start_time = event_logs[i]["event_time"]
                    end_time = event_logs[i]["event_time"]
                else:
                    if (event_logs[i]["event_time"] >
                            end_time + datetime.timedelta(hours=0.5)):
                        session_id = course_learner_id + "_" + \
                            str(start_time) + "_" + str(end_time)
                        duration = ((end_time - start_time).days *
                                    24 * 60 * 60 +
                                    (end_time - start_time).seconds)
                        if duration > 5:
                            array = [
                                session_id, course_learner_id, start_time,
                                end_time, duration]
                            self.session_record.append(array)
                        final_time = event_logs[i]["event_time"]
                        # Re-initialization
                        session_id = ""
                        start_time = event_logs[i]["event_time"]
                        end_time = event_logs[i]["event_time"]
                    else:
                        if event_logs[i]["event_type"] == "page_close":
                            end_time = event_logs[i]["event_time"]
                            session_id = course_learner_id + "_" + \
                                str(start_time) + "_" + str(end_time)
                            duration = ((end_time - start_time).days *
                                        24 * 60 * 60 +
                                        (end_time - start_time).seconds)
                            if duration > 5:
                                array = [
                                    session_id, course_learner_id, start_time,
                                    end_time, duration]
                                self.session_record.append(array)
                            # Re-initialization
                            session_id = ""
                            start_time = ""
                            end_time = ""
                            final_time = event_logs[i]["event_time"]
                        else:
                            end_time = event_logs[i]["event_time"]
            if final_time != "":
                new_logs = []
                for log in event_logs:
                    if log["event_time"] >= final_time:
                        new_logs.append(log)

                self.updated_learner_all_event_logs[
                    course_learner_id] = new_logs

    def handleEvent(self, jsonObject):
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

            if course_learner_id in self.course_learner_id_set:
                self.learner_all_event_logs[course_learner_id].append(
                    {"event_time": event_time, "event_type": event_type})
            else:
                self.learner_all_event_logs[course_learner_id] = [
                    {"event_time": event_time, "event_type": event_type}]
                self.course_learner_id_set.add(course_learner_id)

    def postprocessing(self):
        # Filter duplicated records
        updated_session_record = []
        session_id_set = set()
        for array in self.session_record:
            session_id = array[0]
            if session_id not in session_id_set:
                session_id_set.add(session_id)
                updated_session_record.append(array)

        self.session_record = updated_session_record

        # Database version
        cursor = self.connection.cursor(prepared=True)
        sql = ("insert into "
               "sessions(session_id, course_learner_id, start_time, end_time, "
               "         duration) values (%s,%s,%s,%s,%s)")

        logger.debug("Starting logging %s records to database" %
                     (len(self.session_record)))
        for array in self.session_record:
            session_id = array[0]
            course_learner_id = array[1]
            start_time = array[2]
            end_time = array[3]
            duration = array[4]
            cursor.execute(
                sql, (session_id, course_learner_id,
                      start_time, end_time, duration))

        logger.debug("Finished logging %s records to database" %
                     (len(self.session_record)))