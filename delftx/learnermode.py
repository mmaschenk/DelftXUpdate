from delftx.util import names
import datetime


def process(course_name, base_path, cursor, course_metadata_map):

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
        week = (
          element_start_time - course_metadata_map["start_time"]).days / 7 + 1

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

    print ("The number of enrolled learners is: " +
           str(len(enrolled_learner_set)) + "\n")

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
    input_file = open(names.certificates_generatedcertificate_file(course_name, base_path), "r")
    input_file.readline()
    lines = input_file.readlines()

    for line in lines:
        record = line.split("\t")
        global_learner_id = record[1]
        final_grade = record[3]
        enrollment_mode = record[14].replace("\n", "")
        certificate_status = record[7]

        if course_learner_map.has_key(global_learner_id):
            num_certifiedLearners += 1
            array = [course_learner_map[global_learner_id],
                     final_grade, enrollment_mode, certificate_status]
            course_learner_record.append(array)
        else:
            num_uncertifiedLearners += 1

    input_file.close()

    print "The number of uncertified & certified learners is: " + str(num_uncertifiedLearners) + "\t" + str(num_certifiedLearners) + "\n"

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
                     level_of_education, country, learner_mail_map[global_learner_id]]
            learner_demographic_record.append(array)

    input_file.close()

    # Database version
    # Course table
    for array in course_record:
        course_id = course_metadata_map["course_id"]
        course_name = course_metadata_map["course_name"]
        start_time = course_metadata_map["start_time"]
        end_time = course_metadata_map["end_time"]
        sql = "insert into courses(course_id, course_name, start_time, end_time) values"
        sql += "('%s','%s','%s','%s');" % (course_id,
                                           course_name, start_time, end_time)
        cursor.execute(sql)

    for array in course_element_record:
        element_id = array[0]
        element_type = array[1]
        week = array[2]
        course_id = array[3]
        sql = "insert into course_elements(element_id, element_type, week, course_id) values"
        sql += "('%s','%s','%s','%s');" % (element_id,
                                           element_type, week, course_id)
        cursor.execute(sql)

    # Learner_index table
    for array in learner_index_record:
        global_learner_id = array[0]
        course_id = array[1]
        course_learner_id = array[2]
        sql = "insert into learner_index(global_learner_id, course_id, course_learner_id) values"
        sql += "('%s','%s','%s');" % (global_learner_id,
                                      course_id, course_learner_id)
        cursor.execute(sql)

    # Course_learner table
    for array in course_learner_record:
        course_learner_id = array[0]
        final_grade = array[1]
        enrollment_mode = array[2]
        certificate_status = array[3]
        sql = "insert into course_learner(course_learner_id, final_grade, enrollment_mode, certificate_status) values"
        sql += "('%s','%s','%s','%s');" % (course_learner_id,
                                           final_grade, enrollment_mode, certificate_status)
        cursor.execute(sql)

    # Learner_demographic table
    for array in learner_demographic_record:
        course_learner_id = array[0]
        gender = array[1]
        year_of_birth = array[2]
        level_of_education = array[3]
        country = array[4]
        email = array[5]
        email = email.replace("\'", "")
        sql = "insert into learner_demographic(course_learner_id, gender, year_of_birth, level_of_education, country, email) values"
        sql += "('%s','%s','%s','%s','%s','%s');" % (course_learner_id,
                                                     gender, year_of_birth, level_of_education, country, email)
        cursor.execute(sql)

    # File version
    '''
    pairs = [["courses", course_record], ["course_element", course_element_record], ["learner_index", learner_index_record], ["course_learner", course_learner_record], ["learner_demographic", learner_demographic_record]]
    for pair in pairs:
        output_path = "/Users/Angus/Downloads/" + pair[0]
        output_file = open(output_path, "w")
        writer = csv.writer(output_file)
        for array in pair[1]:
            writer.writerow(array)
        output_file.close()
    '''
