import json
import datetime


def extract(filename):
    course_metadata_map = {}

    course_structure_file = open(filename, "r")

    child_parent_map = {}
    element_time_map = {}
    element_type_map = {}
    element_without_time = []

    quiz_question_array = []
    block_type_map = {}

    jsonObject = json.loads(course_structure_file.read())
    for record in jsonObject:
        if jsonObject[record]["category"] == "course":

            # Course ID
            course_id = record
            if course_id.startswith("block-"):
                course_id = course_id.replace("block-", "course-")
                course_id = course_id.replace("+type@course+block@course", "")
            if course_id.startswith("i4x://"):
                course_id = course_id.replace("i4x://", "")
                course_id = course_id.replace("course/", "")
            course_metadata_map["course_id"] = course_id

            # Course name
            course_name = jsonObject[record]["metadata"]["display_name"]
            course_metadata_map["course_name"] = course_name

            start_time = jsonObject[record]["metadata"]["start"]
            end_time = jsonObject[record]["metadata"]["end"]

            # Start & End data
            start_date = start_time[0:start_time.index("T")]
            end_date = end_time[0:end_time.index("T")]
            course_metadata_map["start_date"] = start_date
            course_metadata_map["end_date"] = end_date

            # Start & End time
            fmt = "%Y-%m-%d %H:%M:%S"

            start_time = start_time[0:19]
            start_time = start_time.replace("T", " ")
            start_time = datetime.datetime.strptime(start_time, fmt)

            end_time = end_time[0:19]
            end_time = end_time.replace("T", " ")
            end_time = datetime.datetime.strptime(end_time, fmt)

            course_metadata_map["start_time"] = start_time
            course_metadata_map["end_time"] = end_time

            for child in jsonObject[record]["children"]:
                child_parent_map[child] = record
            element_time_map[record] = start_time

            element_type_map[record] = jsonObject[record]["category"]

        else:

            element_id = record

            # Child to parent relation
            for child in jsonObject[element_id]["children"]:
                child_parent_map[child] = element_id

            # Element time
            if "start" in jsonObject[element_id]["metadata"]:
                element_start_time = jsonObject[
                    element_id]["metadata"]["start"]
                element_start_time = element_start_time[0:19]
                element_start_time = element_start_time.replace("T", " ")
                element_start_time = datetime.datetime.strptime(
                    element_start_time, "%Y-%m-%d %H:%M:%S")
                element_time_map[element_id] = element_start_time
            else:
                element_without_time.append(element_id)

            # Element type
            element_type_map[element_id] = jsonObject[element_id]["category"]

            # Quiz questions
            if jsonObject[element_id]["category"] == "problem":
                quiz_question_array.append(element_id)

            # Types of blocks to which quiz questions belong
            if jsonObject[element_id]["category"] == "sequential":
                if "display_name" in jsonObject[element_id]["metadata"]:
                    block_type = jsonObject[element_id][
                        "metadata"]["display_name"]
                    block_type_map[element_id] = block_type

    # Decide the start time for each element
    for element_id in element_without_time:
        element_start_time = ""
        while element_start_time == "":
            element_parent = child_parent_map[element_id]
            # while not element_time_map.has_key(element_parent):
            while element_parent not in element_time_map:
                element_parent = child_parent_map[element_parent]
            element_start_time = element_time_map[element_parent]
        element_time_map[element_id] = element_start_time

    # Remove deleted elements
    for element_id in element_time_map.keys():
        if element_time_map[element_id] < course_metadata_map["start_time"]:
            element_time_map.pop(element_id)

    course_metadata_map["element_time_map"] = element_time_map
    course_metadata_map["element_type_map"] = element_type_map
    course_metadata_map["quiz_question_array"] = quiz_question_array
    course_metadata_map["child_parent_map"] = child_parent_map
    course_metadata_map["block_type_map"] = block_type_map

    return course_metadata_map
