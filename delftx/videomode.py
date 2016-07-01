import os
import json
import time
import datetime
import operator
from dateutil.parser import parse
import logging
from translation.Functions import ExtractCourseInformation, getNextDay, cmp_datetime

import delftx.util
from delftx.util import BaseEventProcessor
logger = logging.getLogger(__name__)


def video_interaction(coursename, base_path, course_metadata_map, connection,
                      bufferLocation=None):

    oneday = datetime.timedelta(days=1)

    current_date = parse(course_metadata_map["start_date"]).date()
    end_next_date = parse(course_metadata_map["end_date"]).date() + oneday

    video_interaction_map = {}

    # Video-related event types
    video_event_types = []
    video_event_types.append("play_video")
    video_event_types.append("edx.video.played")
    video_event_types.append("stop_video")
    video_event_types.append("edx.video.stopped")
    video_event_types.append("pause_video")
    video_event_types.append("edx.video.paused")
    video_event_types.append("seek_video")
    video_event_types.append("edx.video.position.changed")
    video_event_types.append("speed_change_video")

    # Navigation-related event types
    navigation_event_types = []
    navigation_event_types.append("page_close")
    navigation_event_types.append("seq_goto")
    navigation_event_types.append("seq_next")
    navigation_event_types.append("seq_prev")

    learner_video_event_logs = {}
    updated_learner_video_event_logs = {}

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

        learner_video_event_logs.clear()
        learner_video_event_logs = updated_learner_video_event_logs.copy()
        updated_learner_video_event_logs.clear()

        # Course_learner_id set
        course_learner_id_set = set()
        for course_learner_id in learner_video_event_logs.keys():
            course_learner_id_set.add(course_learner_id)

        for line in lines:
            jsonObject = json.loads(line)
            if jsonObject["event_type"] in video_event_types:
                global_learner_id = jsonObject["context"]["user_id"]
                if global_learner_id != "":
                    course_id = jsonObject["context"]["course_id"]
                    course_learner_id = course_id + \
                        "_" + str(global_learner_id)
                    video_id = ""
                    event_time = jsonObject["time"]
                    event_time = event_time[0:19]
                    event_time = event_time.replace("T", " ")
                    event_time = datetime.datetime.strptime(
                        event_time, "%Y-%m-%d %H:%M:%S")
                    event_type = jsonObject["event_type"]

                    # For seek event
                    new_time = 0
                    old_time = 0

                    # For speed change event
                    new_speed = 0
                    old_speed = 0

                    # This sub-condition does not exist in log data
                    # if isinstance(jsonObject["event"], dict):
                    #     video_id = jsonObject["event"]["id"]

                    if isinstance(jsonObject["event"], unicode):
                        event_jsonObject = json.loads(
                            jsonObject["event"])
                        video_id = event_jsonObject["id"]

                        video_id = video_id.replace("-", "://", 1)
                        video_id = video_id.replace("-", "/")

                        # For video seek event
                        if ("new_time" in event_jsonObject and
                                "old_time" in event_jsonObject):
                            new_time = event_jsonObject["new_time"]
                            old_time = event_jsonObject["old_time"]

                        # For video speed change event
                        if ("new_speed" in event_jsonObject and
                                "old_speed" in event_jsonObject):
                            new_speed = event_jsonObject["new_speed"]
                            old_speed = event_jsonObject["old_speed"]

                    # To record video seek event
                    if event_type in ["seek_video",
                                      "edx.video.position.changed"]:
                        if new_time is not None and old_time is not None:
                            if course_learner_id in course_learner_id_set:
                                learner_video_event_logs[
                                    course_learner_id].append(
                                        {"event_time": event_time,
                                         "event_type": event_type,
                                         "video_id": video_id,
                                         "new_time": new_time,
                                         "old_time": old_time})
                            else:
                                learner_video_event_logs[course_learner_id] = [
                                    {"event_time": event_time,
                                     "event_type": event_type,
                                     "video_id": video_id,
                                     "new_time": new_time,
                                     "old_time": old_time}]
                                course_learner_id_set.add(
                                    course_learner_id)
                        continue

                    # To record video speed change event
                    if event_type in ["speed_change_video"]:
                        if course_learner_id in course_learner_id_set:
                            learner_video_event_logs[course_learner_id].append(
                                {"event_time": event_time,
                                 "event_type": event_type,
                                 "video_id": video_id,
                                 "new_speed": new_speed,
                                 "old_speed": old_speed})
                        else:
                            learner_video_event_logs[course_learner_id] = [
                                {"event_time": event_time,
                                 "event_type": event_type,
                                 "video_id": video_id,
                                 "new_speed": new_speed,
                                 "old_speed": old_speed}]
                            course_learner_id_set.add(
                                course_learner_id)
                        continue

                    if course_learner_id in course_learner_id_set:
                        learner_video_event_logs[course_learner_id].append(
                            {"event_time": event_time,
                             "event_type": event_type,
                             "video_id": video_id})
                    else:
                        learner_video_event_logs[course_learner_id] = [
                            {"event_time": event_time,
                             "event_type": event_type,
                             "video_id": video_id}]
                        course_learner_id_set.add(course_learner_id)

            # For navigation events
            if jsonObject["event_type"] in navigation_event_types:

                global_learner_id = jsonObject["context"]["user_id"]

                if global_learner_id != "":
                    course_id = jsonObject["context"]["course_id"]
                    course_learner_id = course_id + \
                        "_" + str(global_learner_id)

                    event_time = jsonObject["time"]
                    event_time = event_time[0:19]
                    event_time = event_time.replace("T", " ")
                    event_time = datetime.datetime.strptime(
                        event_time, "%Y-%m-%d %H:%M:%S")

                    event_type = jsonObject["event_type"]

                    if course_learner_id in course_learner_id_set:
                        learner_video_event_logs[course_learner_id].append(
                            {"event_time": event_time,
                             "event_type": event_type})
                    else:
                        learner_video_event_logs[course_learner_id] = [
                            {"event_time": event_time,
                             "event_type": event_type}]
                        course_learner_id_set.add(course_learner_id)
        logger.debug("%s json-lines processed" % (len(lines,)))

        logger.debug("Processing %s learner ids" %
                     (len(learner_video_event_logs.keys())))
        for course_learner_id in learner_video_event_logs.keys():

            video_id = ""

            event_logs = learner_video_event_logs[course_learner_id]

            # Sorting
            event_logs.sort(
                cmp=cmp_datetime, key=operator.itemgetter('event_time'))

            video_start_time = ""
            final_time = ""

            # For video seek event
            times_forward_seek = 0
            duration_forward_seek = 0
            times_backward_seek = 0
            duration_backward_seek = 0

            # For video speed change event
            speed_change_last_time = ""
            times_speed_up = 0
            times_speed_down = 0

            # For video pause event
            pause_check = False
            pause_start_time = ""
            duration_pause = 0

            for log in event_logs:
                if log["event_type"] in ["play_video", "edx.video.played"]:
                    video_start_time = log["event_time"]
                    video_id = log["video_id"]
                    if pause_check:
                        duration_pause = (
                            log["event_time"] - pause_start_time).seconds
                        video_interaction_id = course_learner_id + \
                            "_" + video_id + "_" + \
                            str(pause_start_time)

                        if duration_pause > 2 and duration_pause < 600:
                            if (video_interaction_id in
                                    video_interaction_map.keys()):
                                video_interaction_map[
                                    video_interaction_id]["times_pause"] = 1
                                video_interaction_map[video_interaction_id][
                                    "duration_pause"] = duration_pause
                        pause_check = False
                    continue

                if video_start_time != "":
                    if (log["event_time"] >
                            video_start_time + datetime.timedelta(hours=0.5)):
                        video_start_time = ""
                        video_id = ""
                        final_time = log["event_time"]
                    else:
                        # 0. Seek
                        if (log["event_type"] in
                                ["seek_video",
                                 "edx.video.position.changed"] and
                                video_id == log["video_id"]):
                            # Forward seek event
                            if log["new_time"] > log["old_time"]:
                                times_forward_seek += 1
                                duration_forward_seek += log[
                                    "new_time"] - log["old_time"]
                            # Backward seek event
                            if log["new_time"] < log["old_time"]:
                                times_backward_seek += 1
                                duration_backward_seek += log[
                                    "old_time"] - log["new_time"]
                            continue

                        # 1. Speed change
                        if (log["event_type"] == "speed_change_video" and
                                video_id == log["video_id"]):
                            if speed_change_last_time == "":
                                speed_change_last_time = log[
                                    "event_time"]
                                old_speed = log["old_speed"]
                                new_speed = log["new_speed"]
                                if old_speed < new_speed:
                                    times_speed_up += 1
                                if old_speed > new_speed:
                                    times_speed_down += 1
                            else:
                                if (log["event_time"] -
                                        speed_change_last_time).seconds > 10:
                                    old_speed = log["old_speed"]
                                    new_speed = log["new_speed"]
                                    if old_speed < new_speed:
                                        times_speed_up += 1
                                    if old_speed > new_speed:
                                        times_speed_down += 1
                                speed_change_last_time = log[
                                    "event_time"]
                            continue

                        # 2. Pause/Stop situation
                        if (log["event_type"] in
                                ["pause_video", "edx.video.paused",
                                 "stop_video", "edx.video.stopped"] and
                                video_id == log["video_id"]):

                            watch_duration = (
                                log["event_time"] - video_start_time).seconds

                            video_end_time = log["event_time"]
                            video_interaction_id = course_learner_id + \
                                "_" + video_id + "_" + \
                                str(video_end_time)

                            if watch_duration > 5:
                                video_interaction_map[video_interaction_id] = {
                                    "course_learner_id": course_learner_id,
                                    "video_id": video_id,
                                    "type": "video",
                                    "watch_duration": watch_duration,
                                    "times_forward_seek": times_forward_seek,
                                    "duration_forward_seek":
                                        duration_forward_seek,
                                    "times_backward_seek": times_backward_seek,
                                    "duration_backward_seek":
                                        duration_backward_seek,
                                    "times_speed_up": times_speed_up,
                                    "times_speed_down": times_speed_down,
                                    "start_time": video_start_time,
                                    "end_time": video_end_time}

                            if log["event_type"] in ["pause_video",
                                                     "edx.video.paused"]:
                                pause_check = True
                                pause_start_time = video_end_time

                            # For video seek event
                            times_forward_seek = 0
                            duration_forward_seek = 0
                            times_backward_seek = 0
                            duration_backward_seek = 0

                            # For video speed change event
                            speed_change_last_time = ""
                            times_speed_up = 0
                            times_speed_down = 0

                            # For video general information
                            video_start_time = ""
                            video_id = ""
                            final_time = log["event_time"]

                            continue

                        # 3/4  Page changed/Session closed
                        if log["event_type"] in navigation_event_types:

                            video_end_time = log["event_time"]
                            watch_duration = (
                                video_end_time - video_start_time).seconds
                            video_interaction_id = course_learner_id + \
                                "_" + video_id + "_" + \
                                str(video_end_time)

                            if watch_duration > 5:
                                video_interaction_map[video_interaction_id] = {
                                    "course_learner_id": course_learner_id,
                                    "video_id": video_id, "type": "video",
                                    "watch_duration": watch_duration,
                                    "times_forward_seek": times_forward_seek,
                                    "duration_forward_seek":
                                        duration_forward_seek,
                                    "times_backward_seek": times_backward_seek,
                                    "duration_backward_seek":
                                        duration_backward_seek,
                                    "times_speed_up": times_speed_up,
                                    "times_speed_down": times_speed_down,
                                    "start_time": video_start_time,
                                    "end_time": video_end_time}

                            # For video seek event
                            times_forward_seek = 0
                            duration_forward_seek = 0
                            times_backward_seek = 0
                            duration_backward_seek = 0

                            # For video speed change event
                            speed_change_last_time = ""
                            times_speed_up = 0
                            times_speed_down = 0

                            # For video general information
                            video_start_time = ""
                            video_id = ""
                            final_time = log["event_time"]

                            continue

            if final_time != "":
                new_logs = []
                for log in event_logs:
                    if log["event_time"] > final_time:
                        new_logs.append(log)

                updated_learner_video_event_logs[course_learner_id] = new_logs

        current_date = current_date + oneday

    video_interaction_record = []

    logger.debug("Processing %s video interactions" %
                 (len(video_interaction_map.keys())))
    for interaction_id in video_interaction_map.keys():
        video_interaction_id = interaction_id
        course_learner_id = video_interaction_map[
            interaction_id]["course_learner_id"]
        video_id = video_interaction_map[interaction_id]["video_id"]
        duration = video_interaction_map[interaction_id]["watch_duration"]
        times_forward_seek = video_interaction_map[
            interaction_id]["times_forward_seek"]
        duration_forward_seek = video_interaction_map[
            interaction_id]["duration_forward_seek"]
        times_backward_seek = video_interaction_map[
            interaction_id]["times_backward_seek"]
        duration_backward_seek = video_interaction_map[
            interaction_id]["duration_backward_seek"]
        times_speed_up = video_interaction_map[
            interaction_id]["times_speed_up"]
        times_speed_down = video_interaction_map[
            interaction_id]["times_speed_down"]
        start_time = video_interaction_map[interaction_id]["start_time"]
        end_time = video_interaction_map[interaction_id]["end_time"]

        if "times_pause" in video_interaction_map[interaction_id]:
            times_pause = video_interaction_map[
                interaction_id]["watch_duration"]
            duration_pause = video_interaction_map[
                interaction_id]["watch_duration"]
        else:
            times_pause = 0
            duration_pause = 0

        array = [video_interaction_id, course_learner_id, video_id, duration,
                 times_forward_seek, duration_forward_seek,
                 times_backward_seek, duration_backward_seek, times_speed_up,
                 times_speed_down, times_pause, duration_pause, start_time,
                 end_time]
        video_interaction_record.append(array)

    # Video_interaction table
    # Database version
    cursor = connection.cursor(prepared=True)
    sql = ("insert into "
           "video_interaction(interaction_id, course_learner_id, video_id, "
           "                  duration, times_forward_seek, "
           "                  duration_forward_seek, times_backward_seek, "
           "                  duration_backward_seek, times_speed_up, "
           "                  times_speed_down, times_pause, duration_pause, "
           "                  start_time, end_time) "
           "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

    logger.debug("Starting logging %s video_interaction entries to database" %
                 (len(video_interaction_record)))

    for array in video_interaction_record:
        interaction_id = array[0]
        course_learner_id = array[1]
        video_id = array[2]
        duration = array[3]
        times_forward_seek = array[4]
        duration_forward_seek = array[5]
        times_backward_seek = array[6]
        duration_backward_seek = array[7]
        times_speed_up = array[8]
        times_speed_down = array[9]
        times_pause = array[10]
        duration_pause = array[11]
        start_time = array[12]
        end_time = array[13]

        cursor.execute(sql,
                       (interaction_id, course_learner_id, video_id, duration,
                        times_forward_seek, duration_forward_seek,
                        times_backward_seek, duration_backward_seek,
                        times_speed_up, times_speed_down, times_pause,
                        duration_pause, start_time, end_time)
                       )

    logger.debug("Finished logging %s video_interaction entries to database" %
                 (len(video_interaction_record)))


class VideoInteraction(BaseEventProcessor):

    def __init__(self, course_metadata_map, base_path, connection):
        super(VideoInteraction,
              self).__init__(course_metadata_map, base_path, connection)

        self.video_interaction_map = {}
        self.learner_video_event_logs = {}
        self.updated_learner_video_event_logs = {}

    # Video-related event types
    video_event_types = []
    video_event_types.append("play_video")
    video_event_types.append("edx.video.played")
    video_event_types.append("stop_video")
    video_event_types.append("edx.video.stopped")
    video_event_types.append("pause_video")
    video_event_types.append("edx.video.paused")
    video_event_types.append("seek_video")
    video_event_types.append("edx.video.position.changed")
    video_event_types.append("speed_change_video")

    # Navigation-related event types
    navigation_event_types = []
    navigation_event_types.append("page_close")
    navigation_event_types.append("seq_goto")
    navigation_event_types.append("seq_next")
    navigation_event_types.append("seq_prev")

    def init_next_file(self):
        self.learner_video_event_logs.clear()
        self.learner_video_event_logs = \
            self.updated_learner_video_event_logs.copy()
        self.updated_learner_video_event_logs.clear()

        # Course_learner_id set
        self.course_learner_id_set = set()
        for course_learner_id in self.learner_video_event_logs.keys():
            self.course_learner_id_set.add(course_learner_id)

    def post_next_file(self):
        logger.debug("Processing %s learner ids" %
                     (len(self.learner_video_event_logs.keys())))
        for course_learner_id in self.learner_video_event_logs.keys():

            video_id = ""

            event_logs = self.learner_video_event_logs[course_learner_id]

            # Sorting
            event_logs.sort(
                cmp=cmp_datetime, key=operator.itemgetter('event_time'))

            video_start_time = ""
            final_time = ""

            # For video seek event
            times_forward_seek = 0
            duration_forward_seek = 0
            times_backward_seek = 0
            duration_backward_seek = 0

            # For video speed change event
            speed_change_last_time = ""
            times_speed_up = 0
            times_speed_down = 0

            # For video pause event
            pause_check = False
            pause_start_time = ""
            duration_pause = 0

            for log in event_logs:
                if log["event_type"] in ["play_video", "edx.video.played"]:
                    video_start_time = log["event_time"]
                    video_id = log["video_id"]
                    if pause_check:
                        duration_pause = (
                            log["event_time"] - pause_start_time).seconds
                        video_interaction_id = course_learner_id + \
                            "_" + video_id + "_" + \
                            str(pause_start_time)

                        if duration_pause > 2 and duration_pause < 600:
                            if (video_interaction_id in
                                    self.video_interaction_map.keys()):
                                self.video_interaction_map[
                                    video_interaction_id]["times_pause"] = 1
                                self.video_interaction_map[
                                    video_interaction_id][
                                    "duration_pause"] = duration_pause
                        pause_check = False
                    continue

                if video_start_time != "":
                    if (log["event_time"] >
                            video_start_time + datetime.timedelta(hours=0.5)):
                        video_start_time = ""
                        video_id = ""
                        final_time = log["event_time"]
                    else:
                        # 0. Seek
                        if (log["event_type"] in
                                ["seek_video",
                                 "edx.video.position.changed"] and
                                video_id == log["video_id"]):
                            # Forward seek event
                            if log["new_time"] > log["old_time"]:
                                times_forward_seek += 1
                                duration_forward_seek += log[
                                    "new_time"] - log["old_time"]
                            # Backward seek event
                            if log["new_time"] < log["old_time"]:
                                times_backward_seek += 1
                                duration_backward_seek += log[
                                    "old_time"] - log["new_time"]
                            continue

                        # 1. Speed change
                        if (log["event_type"] == "speed_change_video" and
                                video_id == log["video_id"]):
                            if speed_change_last_time == "":
                                speed_change_last_time = log[
                                    "event_time"]
                                old_speed = log["old_speed"]
                                new_speed = log["new_speed"]
                                if old_speed < new_speed:
                                    times_speed_up += 1
                                if old_speed > new_speed:
                                    times_speed_down += 1
                            else:
                                if (log["event_time"] -
                                        speed_change_last_time).seconds > 10:
                                    old_speed = log["old_speed"]
                                    new_speed = log["new_speed"]
                                    if old_speed < new_speed:
                                        times_speed_up += 1
                                    if old_speed > new_speed:
                                        times_speed_down += 1
                                speed_change_last_time = log[
                                    "event_time"]
                            continue

                        # 2. Pause/Stop situation
                        if (log["event_type"] in
                                ["pause_video", "edx.video.paused",
                                 "stop_video", "edx.video.stopped"] and
                                video_id == log["video_id"]):

                            watch_duration = (
                                log["event_time"] - video_start_time).seconds

                            video_end_time = log["event_time"]
                            video_interaction_id = course_learner_id + \
                                "_" + video_id + "_" + \
                                str(video_end_time)

                            if watch_duration > 5:
                                self.video_interaction_map[
                                    video_interaction_id] = {
                                    "course_learner_id": course_learner_id,
                                    "video_id": video_id,
                                    "type": "video",
                                    "watch_duration": watch_duration,
                                    "times_forward_seek": times_forward_seek,
                                    "duration_forward_seek":
                                        duration_forward_seek,
                                    "times_backward_seek": times_backward_seek,
                                    "duration_backward_seek":
                                        duration_backward_seek,
                                    "times_speed_up": times_speed_up,
                                    "times_speed_down": times_speed_down,
                                    "start_time": video_start_time,
                                    "end_time": video_end_time}

                            if log["event_type"] in ["pause_video",
                                                     "edx.video.paused"]:
                                pause_check = True
                                pause_start_time = video_end_time

                            # For video seek event
                            times_forward_seek = 0
                            duration_forward_seek = 0
                            times_backward_seek = 0
                            duration_backward_seek = 0

                            # For video speed change event
                            speed_change_last_time = ""
                            times_speed_up = 0
                            times_speed_down = 0

                            # For video general information
                            video_start_time = ""
                            video_id = ""
                            final_time = log["event_time"]

                            continue

                        # 3/4  Page changed/Session closed
                        if log["event_type"] in self.navigation_event_types:

                            video_end_time = log["event_time"]
                            watch_duration = (
                                video_end_time - video_start_time).seconds
                            video_interaction_id = course_learner_id + \
                                "_" + video_id + "_" + \
                                str(video_end_time)

                            if watch_duration > 5:
                                self.video_interaction_map[
                                    video_interaction_id] = {
                                    "course_learner_id": course_learner_id,
                                    "video_id": video_id, "type": "video",
                                    "watch_duration": watch_duration,
                                    "times_forward_seek": times_forward_seek,
                                    "duration_forward_seek":
                                        duration_forward_seek,
                                    "times_backward_seek": times_backward_seek,
                                    "duration_backward_seek":
                                        duration_backward_seek,
                                    "times_speed_up": times_speed_up,
                                    "times_speed_down": times_speed_down,
                                    "start_time": video_start_time,
                                    "end_time": video_end_time}

                            # For video seek event
                            times_forward_seek = 0
                            duration_forward_seek = 0
                            times_backward_seek = 0
                            duration_backward_seek = 0

                            # For video speed change event
                            speed_change_last_time = ""
                            times_speed_up = 0
                            times_speed_down = 0

                            # For video general information
                            video_start_time = ""
                            video_id = ""
                            final_time = log["event_time"]

                            continue

            if final_time != "":
                new_logs = []
                for log in event_logs:
                    if log["event_time"] > final_time:
                        new_logs.append(log)

                self.updated_learner_video_event_logs[
                    course_learner_id] = new_logs

    def handleEvent(self, jsonObject):
        if jsonObject["event_type"] in self.video_event_types:
            global_learner_id = jsonObject["context"]["user_id"]
            if global_learner_id != "":
                course_id = jsonObject["context"]["course_id"]
                course_learner_id = course_id + \
                    "_" + str(global_learner_id)
                video_id = ""
                event_time = jsonObject["time"]
                event_time = event_time[0:19]
                event_time = event_time.replace("T", " ")
                event_time = datetime.datetime.strptime(
                    event_time, "%Y-%m-%d %H:%M:%S")
                event_type = jsonObject["event_type"]

                # For seek event
                new_time = 0
                old_time = 0

                # For speed change event
                new_speed = 0
                old_speed = 0

                # This sub-condition does not exist in log data
                # if isinstance(jsonObject["event"], dict):
                #     video_id = jsonObject["event"]["id"]

                if isinstance(jsonObject["event"], unicode):
                    event_jsonObject = json.loads(
                        jsonObject["event"])
                    video_id = event_jsonObject["id"]

                    video_id = video_id.replace("-", "://", 1)
                    video_id = video_id.replace("-", "/")

                    # For video seek event
                    if ("new_time" in event_jsonObject and
                            "old_time" in event_jsonObject):
                        new_time = event_jsonObject["new_time"]
                        old_time = event_jsonObject["old_time"]

                    # For video speed change event
                    if ("new_speed" in event_jsonObject and
                            "old_speed" in event_jsonObject):
                        new_speed = event_jsonObject["new_speed"]
                        old_speed = event_jsonObject["old_speed"]

                # To record video seek event
                if event_type in ["seek_video",
                                  "edx.video.position.changed"]:
                    if new_time is not None and old_time is not None:
                        if course_learner_id in self.course_learner_id_set:
                            self.learner_video_event_logs[
                                course_learner_id].append(
                                    {"event_time": event_time,
                                     "event_type": event_type,
                                     "video_id": video_id,
                                     "new_time": new_time,
                                     "old_time": old_time})
                        else:
                            self.learner_video_event_logs[
                                course_learner_id] = [
                                {"event_time": event_time,
                                 "event_type": event_type,
                                 "video_id": video_id,
                                 "new_time": new_time,
                                 "old_time": old_time}]
                            self.course_learner_id_set.add(
                                course_learner_id)
                    return

                # To record video speed change event
                if event_type in ["speed_change_video"]:
                    if course_learner_id in self.course_learner_id_set:
                        self.learner_video_event_logs[
                            course_learner_id].append(
                            {"event_time": event_time,
                             "event_type": event_type,
                             "video_id": video_id,
                             "new_speed": new_speed,
                             "old_speed": old_speed})
                    else:
                        self.learner_video_event_logs[course_learner_id] = [
                            {"event_time": event_time,
                             "event_type": event_type,
                             "video_id": video_id,
                             "new_speed": new_speed,
                             "old_speed": old_speed}]
                        self.course_learner_id_set.add(
                            course_learner_id)
                    return

                if course_learner_id in self.course_learner_id_set:
                    self.learner_video_event_logs[course_learner_id].append(
                        {"event_time": event_time,
                         "event_type": event_type,
                         "video_id": video_id})
                else:
                    self.learner_video_event_logs[course_learner_id] = [
                        {"event_time": event_time,
                         "event_type": event_type,
                         "video_id": video_id}]
                    self.course_learner_id_set.add(course_learner_id)

        # For navigation events
        if jsonObject["event_type"] in self.navigation_event_types:

            global_learner_id = jsonObject["context"]["user_id"]

            if global_learner_id != "":
                course_id = jsonObject["context"]["course_id"]
                course_learner_id = course_id + \
                    "_" + str(global_learner_id)

                event_time = jsonObject["time"]
                event_time = event_time[0:19]
                event_time = event_time.replace("T", " ")
                event_time = datetime.datetime.strptime(
                    event_time, "%Y-%m-%d %H:%M:%S")

                event_type = jsonObject["event_type"]

                if course_learner_id in self.course_learner_id_set:
                    self.learner_video_event_logs[course_learner_id].append(
                        {"event_time": event_time,
                         "event_type": event_type})
                else:
                    self.learner_video_event_logs[course_learner_id] = [
                        {"event_time": event_time,
                         "event_type": event_type}]
                    self.course_learner_id_set.add(course_learner_id)

    def postprocessing(self):
        video_interaction_record = []

        logger.debug("Processing %s video interactions" %
                     (len(self.video_interaction_map.keys())))
        for interaction_id in self.video_interaction_map.keys():
            video_interaction_id = interaction_id
            course_learner_id = self.video_interaction_map[
                interaction_id]["course_learner_id"]
            video_id = self.video_interaction_map[interaction_id]["video_id"]
            duration = self.video_interaction_map[
                interaction_id]["watch_duration"]
            times_forward_seek = self.video_interaction_map[
                interaction_id]["times_forward_seek"]
            duration_forward_seek = self.video_interaction_map[
                interaction_id]["duration_forward_seek"]
            times_backward_seek = self.video_interaction_map[
                interaction_id]["times_backward_seek"]
            duration_backward_seek = self.video_interaction_map[
                interaction_id]["duration_backward_seek"]
            times_speed_up = self.video_interaction_map[
                interaction_id]["times_speed_up"]
            times_speed_down = self.video_interaction_map[
                interaction_id]["times_speed_down"]
            start_time = self.video_interaction_map[
                interaction_id]["start_time"]
            end_time = self.video_interaction_map[interaction_id]["end_time"]

            if "times_pause" in self.video_interaction_map[interaction_id]:
                times_pause = self.video_interaction_map[
                    interaction_id]["watch_duration"]
                duration_pause = self.video_interaction_map[
                    interaction_id]["watch_duration"]
            else:
                times_pause = 0
                duration_pause = 0

            array = [video_interaction_id, course_learner_id, video_id,
                     duration, times_forward_seek, duration_forward_seek,
                     times_backward_seek, duration_backward_seek,
                     times_speed_up,
                     times_speed_down, times_pause, duration_pause, start_time,
                     end_time]
            video_interaction_record.append(array)

        # Video_interaction table
        # Database version
        cursor = self.connection.cursor(prepared=True)
        sql = ("insert into "
               "video_interaction(interaction_id, course_learner_id, video_id,"
               "                  duration, times_forward_seek, "
               "                  duration_forward_seek, times_backward_seek, "
               "                  duration_backward_seek, times_speed_up, "
               "                  times_speed_down, times_pause, "
               "                  duration_pause, start_time, end_time) "
               "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

        logger.debug("Starting logging %s video_interaction "
                     "entries to database" % (len(video_interaction_record)))

        for array in video_interaction_record:
            interaction_id = array[0]
            course_learner_id = array[1]
            video_id = array[2]
            duration = array[3]
            times_forward_seek = array[4]
            duration_forward_seek = array[5]
            times_backward_seek = array[6]
            duration_backward_seek = array[7]
            times_speed_up = array[8]
            times_speed_down = array[9]
            times_pause = array[10]
            duration_pause = array[11]
            start_time = array[12]
            end_time = array[13]

            cursor.execute(sql,
                           (interaction_id, course_learner_id, video_id,
                            duration,
                            times_forward_seek, duration_forward_seek,
                            times_backward_seek, duration_backward_seek,
                            times_speed_up, times_speed_down, times_pause,
                            duration_pause, start_time, end_time)
                           )

        logger.debug("Finished logging %s video_interaction entries "
                     "to database" % (len(video_interaction_record)))