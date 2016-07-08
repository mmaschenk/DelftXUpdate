import logging

logger = logging.getLogger(__name__)


class EventlogProcessor(object):

    def __init__(self, course_metadata_map, base_path, connection):
        super(EventlogProcessor, self).__init__()
        self.course_metadata_map = course_metadata_map
        self.base_path = base_path
        self.connection = connection

    def init_next_file(self):
        logger.warn('Unimplemented init_next_file method')

    def post_next_file(self):
        logger.debug('Unimplemented post_next_file method')

    def handleEvent(self, jsonObject):
        raise Exception(
            'handleevent not implemented on ', self.__class__.__name__)

    def postprocessing(self):
        logger.warn('Unimplemented postprocessing method')
