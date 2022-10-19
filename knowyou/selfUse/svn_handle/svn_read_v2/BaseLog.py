# -*- coding: utf-8 -*-

import os
import subprocess
import io
import time

# svn log -v -r {2022-01-01}:{2022-11-01} svn://172.16.1.2/Repo1/MobileBox/Sources/RecommAlgorithm/SparkStandardRecommend --username songk --password "sK&5HiGkqislq*19*x"
# svn log -v -r {2022-01-01}:{2022-11-01} svn://218.80.1.114:6689/Repo1/CMCDP/Sources/ --username qianym --password "Ou2*35&1.5H1Bbwq1zhY"


class BaseLog:
    def __init__(self):
        self.revision = ""
        self.author = ""
        self.time = ""
        self.message = ""
        self.tuple_list = []

    def set_revision(self, revision):
        self.revision = revision

    def set_author(self, author):
        self.author = author

    def set_time(self, time):
        self.time = time

    def set_message(self, msg):
        self.message = msg

    def set_tuple_list(self, tuple_list):
        self.tuple_list = tuple_list

    def get_revision(self):
        return self.revision

    def get_author(self):
        return self.author

    def get_time(self):
        return self.time

    def get_message(self):
        return self.message

    def get_tuple_list(self):
        return self.tuple_list

    def __str__(self):
        if self.revision == '' and self.time == '' and self.author == '' and self.message == '' and self.tuple_list == []:
            return ''

        str = ("%s##%s##%s##%s##%s") % (
            self.revision, self.time, self.author, self.message, self.tuple_list)
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

    log = BaseLog()
    message = ""
    tuple_list = []
    result = []

    for line in res.splitlines():
        if line is None or len(line) < 1:
            continue
        if '--------' in line:
            continue

        if line.count("|") >= 3:
            log.set_tuple_list(tuple_list)
            log.set_message(message)
            tuple_list = []
            message = ""

            if len(log.get_message()) > 0:
                result.append(log)

            log = BaseLog()
            tmp_list = line.split("|")
            log.set_revision(tmp_list[0])
            log.set_author(tmp_list[1])
            log.set_time(tmp_list[2])
        else:
            if 'Changed paths' in line:
                continue

            flag1 = line.lstrip().startswith('A')
            flag2 = line.lstrip().startswith('D')
            flag3 = line.lstrip().startswith('M')

            if flag1 or flag2 or flag3:
                action = line.lstrip()[0]
                path = line.lstrip()[2:]

                tuple_list.append((path, action))
            else:
                if message == "":
                    message = message + line
                else:
                    message = message + '\n' + line

    log.set_tuple_list(tuple_list)
    log.set_message(message)
    result.append(log)

    return result


def read_svn_log(rc, window):
    svn_path = rc.svn_path
    start_date = rc.start_date
    end_date = rc.end_date
    username = rc.username
    password = rc.password

    command = "svn log -v -r " + start_date + ":" + end_date + " " + svn_path + ' --username ' + username + ' --password "' + password + '"'
    import PySimpleGUI as sg
    # sg.cprint(command)
    # window.Refresh()
    # log_list = os.popen(command)
    # res = log_list.read()
    # log_list.close()

    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    time.sleep(2)
    #proc.wait()
    res = proc.communicate()[0]

    # stream_stdout = io.TextIOWrapper(proc.stdout, encoding='utf-8')
    # stream_stderr = io.TextIOWrapper(proc.stderr, encoding='utf-8')

    # res = str(stream_stdout.read(), encoding='unicode_escape')
    # err = str(stream_stderr.read(), encoding='unicode_escape')

#    res = str(proc.stdout.read().decode('gbk'))

    # err = str(proc.stderr.read().decode('utf-8'))

    print(res)
    # print(err)

    if res.startswith('svn: E') and '--------' not in res:
        return ['err', res]

    log = BaseLog()
    message = ""
    tuple_list = []
    result = []

    for line in res.splitlines():
        if line is None or len(line) < 1:
            continue
        if '--------' in line:
            continue

        if line.count("|") >= 3:
            log.set_tuple_list(tuple_list)
            log.set_message(message)
            tuple_list = []
            message = ""

            if len(log.get_message()) > 0:
                result.append(log)

            log = BaseLog()
            tmp_list = line.split("|")
            log.set_revision(tmp_list[0])
            log.set_author(tmp_list[1])
            log.set_time(tmp_list[2])
        else:
            if 'Changed paths' in line:
                continue

            flag1 = line.lstrip().startswith('A')
            flag2 = line.lstrip().startswith('D')
            flag3 = line.lstrip().startswith('M')

            if flag1 or flag2 or flag3:
                action = line.lstrip()[0]
                path = line.lstrip()[2:]

                tuple_list.append((path, action))
            else:
                if message == "":
                    message = message + line
                else:
                    message = message + '\n' + line

    log.set_tuple_list(tuple_list)
    log.set_message(message)
    result.append(log)

    # r = [str(i) for i in result]
    # print(r)
    return result


if __name__ == '__main__':
    # dir = "svn://172.16.1.2/Repo1/MobileBox/Sources/RecommAlgorithm/"
    # start_date = "{2022-01-01}"
    # end_date = "{2022-11-01}"
    #
    # logs = read_svn_log(dir, start_date, end_date)

    from svn_read_v1.ReadConfig import ReadConfig

    rc = ReadConfig()
    logs = read_svn_log(rc)
    for i in logs:
        print(str(i))

    import time
    time.sleep(5)
    # import xlwt
    #
    # book = xlwt.Workbook(encoding='utf-8')
    # sheet = book.add_sheet("log", cell_overwrite_ok=True)
    #
    # sheet.write(0, 0, 'Revision')
    # sheet.write(0, 1, 'Time')
    # sheet.write(0, 2, 'Author')
    # sheet.write(0, 3, 'Message')
    # sheet.write(0, 4, 'Path_Action')
    #
    # i = 1
    # for log in logs:
    #     j = 0
    #     for col in str(log).split('##'):
    #         sheet.write(i, j, col)
    #         j = j + 1
    #     i = i + 1
    #
    # book.save(r'D:\a-sk\svn_log_read.xls')
