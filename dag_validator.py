import os
import re
from datetime import datetime
from glob import glob
from itertools import chain

DIR = os.path.dirname(os.path.abspath(__file__))
DAG_FOLDER = os.path.join(DIR, 'dags')


def list_dags():
    return (chain.from_iterable(
        glob(os.path.join(x[0], '*_DAG.py')) for x in os.walk(DAG_FOLDER)))


def has_description(f):
    f.seek(0, 0)
    return 'description' in f.read()


def has_start_date(f):
    f.seek(0, 0)
    return 'start_date' in f.read()


def has_execution_timeout(f):
    f.seek(0, 0)
    return 'execution_timeout' in f.read()


def get_datetime(f):
    f.seek(0, 0)
    regex = re.compile(r"[\'\"]start_date[\'\"][:]\sdatetime\((.*)\)")

    match = regex.search(f.read()).groups()[0]
    date = [int(x) for x in match.strip(' ').split(',')]

    year = date[0]
    month = date[1]
    day = date[2]
    hour = date[3] if len(date) > 3 else 0
    minute = date[4] if len(date) > 3 else 0
    second = date[5] if len(date) > 3 else 0

    return datetime(year, month, day, hour, minute, second)


def import_path_not_updated(f):
    f.seek(0, 0)
    return 'from jobs' in f.read()


def validate(dag):
    errs = 0
    name = os.path.basename(dag)

    print("INFO validating dag '{}'...".format(name))

    with open(dag) as f:
        if not has_description(f):
            print("WARN dag '{}' has no description!".format(name))


#            print "     ... please include a description so others can" \
#                  " understand what it is supposed to do!"

        if not has_start_date(f):
            errs += 1
            print("ERROR dag '{}' has no start date set!".format(name))
            print("     ... new dags should have start date set to after" \
                  " 2017/12/14")

        start_date = get_datetime(f)

        if start_date < datetime(2017, 12, 14, 0, 0, 0):
            errs += 1
            print("ERROR dag '{}' start date is too old ({})!".format(
                name, start_date))
            print("     ... new dags should have start date set to after" \
                  " 2017/12/14")
            print("     ... otherwise this may cause Airflow to queue"\
                  " hundreds of executions")

        if import_path_not_updated(f):
            errs += 1
            print("ERROR dag '{}' is importing 'jobs.*' when it should" \
                  " be importing 'dags.jobs.*'!".format(name))

        if not has_execution_timeout(f):
            errs += 1
            print("ERROR dag '{}' does not have an execution timeout!" \
                .format(name))

    return errs


def validate_dags():
    errs = 0

    for dag in list_dags():
        errs += validate(dag)

    print(errs, "errors, all good!")

    if errs > 0:
        print
        raise Exception(
            "There are {} dag validation errors! Fix them!".format(errs))


if __name__ == "__main__":
    validate_dags()