# -*- coding: utf-8 -*-

import xlwt
import os


class baseLog:
    def __init__(self):
        self.revision = ""
        self.author = ""
        self.time = ""
        self.message = ""

    def set_revision(self, revision):
        self.revision = revision

    def set_author(self, author):
        self.author = author

    def set_time(self, time):
        self.time = time

    def set_message(self, msg):
        self.message = msg

    def get_revision(self):
        return self.revision

    def get_author(self):
        return self.author

    def get_time(self):
        return self.time

    def get_message(self):
        return self.message

    def __str__(self):
        str = ("Revision : %s\nTime : %s\nAuthor : %s\nMessage : %s\n") % (
            self.revision, self.time, self.author, self.message)
        return str


def read_svn_log(dir, start_date, end_date):
    if start_date is None:
        command = "svn log -v " + dir
    else:
        command = "svn log -v -r " + start_date + ":" + end_date + " " + dir

    print(command)

    log_list = os.popen(command)

    res = log_list.read()

    log_list.close()

    i = 0
    log = baseLog()
    message = ""
    result = []

    for line in res.splitlines():
        if line is None or len(line) < 1:
            continue
        if '--------' in line:
            continue

        i = i + 1
        if line.count("|") >= 3:
            log.set_message(message)
            message = ""

            if len(log.get_message()) > 0:
                result.append(log)

            log = baseLog()
            tmp_list = line.split("|")
            log.set_revision(tmp_list[0])
            log.set_author(tmp_list[1])
            log.set_time(tmp_list[2])
        else:
            message = message + line

    log.set_message(message)
    result.append(log)

    return result


if __name__ == "__main__":
    dir = "svn://172.16.1.2/Repo1/MobileBox/Sources/RecommAlgorithm/Songk/test_svn/"
    start_date = "{2022-01-01}"
    end_date = "{2022-11-01}"
    res = read_svn_log(dir, start_date, end_date)

    for i in res:
        print(str(i))

    print('-' * 8)

    r = [str(i) for i in res]
    print(r)
