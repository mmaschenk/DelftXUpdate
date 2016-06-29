import os

organization = 'DelftX'
site = 'prod'
loglocation = 'log-data/edx/events'
logbasename = 'delftx-edx-events'


def edxfile(base_path, name, organization, contents, site, filetype):
    return os.path.join(
        base_path, name,
        '-'.join([organization, name, contents, site, filetype]))


def course_structure_file(coursename, base_path):
    return edxfile(base_path, coursename, organization,
                   'course_structure', site, 'analytics.json')


def course_enrollment_file(coursename, base_path):
    return edxfile(base_path, coursename, organization,
                   'student_courseenrollment', site, 'analytics.sql')


def auth_user_file(coursename, base_path):
    return edxfile(base_path, coursename, organization,
                   'auth_user', site, 'analytics.sql')


def auth_userprofile_file(coursename, base_path):
    return edxfile(base_path, coursename, organization,
                   'auth_userprofile', site, 'analytics.sql')


def certificates_generatedcertificate_file(coursename, base_path):
    return edxfile(base_path, coursename, organization,
                   'certificates_generatedcertificate', site, 'analytics.sql')


def mongo_file(coursename, base_path):
    return os.path.join(
        base_path, coursename,
        '-'.join([organization, coursename, site + '.mongo']))


def eventlogfile(date, basepath):
    return os.path.join(basepath,
                        loglocation,
                        str(date.year),
                        logbasename + '-' + date.isoformat() + '.log.gz')
