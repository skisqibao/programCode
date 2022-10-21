# -*- coding: utf-8 -*-

import os
import subprocess
import io
import sys
import time
import requests
import time
import rsa as r
from Crypto.Cipher import AES


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


def read_svn_log(rc):
    svn_path = rc.svn_path
    start_date = rc.start_date
    end_date = rc.end_date
    username = rc.username
    password = rc.password

    command = "svn log -v -r " + start_date + ":" + end_date + " " + svn_path + ' --username ' + username + ' --password "' + password + '"'
    # import PySimpleGUI as sg
    print(command)
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
    # proc.wait()
    res = proc.communicate()[0]

    # stream_stdout = io.TextIOWrapper(proc.stdout, encoding='utf-8')
    # stream_stderr = io.TextIOWrapper(proc.stderr, encoding='utf-8')

    # res = str(stream_stdout.read(), encoding='unicode_escape')
    # err = str(stream_stderr.read(), encoding='unicode_escape')

    #    res = str(proc.stdout.read().decode('gbk'))

    # err = str(proc.stderr.read().decode('utf-8'))

    # print(res)
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
            if log.get_author():
                result.append(log)

            tuple_list = []
            message = ""

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


def write_2_xls(logs, path):
    import xlwt

    book = xlwt.Workbook(encoding='utf-8')
    sheet = book.add_sheet("log", cell_overwrite_ok=True)

    sheet.write(0, 0, 'Revision')
    sheet.write(0, 1, 'Time')
    sheet.write(0, 2, 'Author')
    sheet.write(0, 3, 'Message')
    sheet.write(0, 4, 'Path_Action')

    i = 1
    for log in logs:
        j = 0
        for col in str(log).split('##'):
            sheet.write(i, j, col)
            j = j + 1
        i = i + 1

    book.save(r'D:\a-sk\svn_log_read.xls')


def get_employee_dict():
    dirname = os.path.split(os.path.realpath(sys.argv[0]))[0]
    file_name = "employee.dict"
    dict_path = os.path.join(dirname, file_name)

    employee_dict = {}
    with open(dict_path, encoding='utf-8') as f:
        for line in f:
            (account, employee) = line.split(' ')
            employee_dict[account] = employee

    return employee_dict


def pad(text):
    while len(text) % 16 != 0:
        text += b' '
    return text


def checkout(user):
    crypto_k = b'\xaa\xb7\xbe\xca\xd7\xcb\xdf\xeb\xf5L3\x14@\xeb\xbc\xa9\xae\x9e.\x12Sn\xa8g\xc61\xdew\xf0\xc0\xbc\x962\x198_\x02\xe2r\x96\xa6\xa3\x01\xb9\xa49E2\xe39\xd1\xea\xfa\x10\xbbL(\xaa\x99\xc9\xb2Y\x00u'

    pri = r.key.PrivateKey(
        9713393704512592157546430310860486664114505678414123585336016233443728202317854157740396526919533440289467829889178555370714404778318525826866798782871251,
        65537,
        9409558358154797111649148132285574817959894746562666342100304600647603216401446776298034390559736785751701892745921951246754282388407311679178404137435873,
        6099705285068143943341045826288377554551761782507434759199582469302017852334027811,
        1592436560548363813414997265037835962321617015356973333675788271476721041)
    flag1 = b'\x84]50=jS\x0b[,S#\xebwc\xf5'
    k = r.decrypt(crypto_k, pri)
    cipher = AES.new(k, AES.MODE_ECB)
    flag2 = cipher.encrypt(pad(user.encode()))

    return flag1 == flag2


def send_ding_msg(title, content, at_phone_list):
    '''
curl 'https://oapi.dingtalk.com/robot/send?access_token=5d28b13ac0ae5a59e913204e629d4822bf3c64abdb826b971d5389d3815bea4e' \
-H 'Content-Type: application/json' \
-d '{"msgtype": "text","text": {"content":"svn test test"}}'

    :param title:
    :param content:
    :return:
    '''

    ding_webhook = "https://oapi.dingtalk.com/robot/send?access_token=5d28b13ac0ae5a59e913204e629d4822bf3c64abdb826b971d5389d3815bea4e"

    data = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"svn{title}",
            "text": f"{content} \n"
        },
        "at": {
            "atMobiles": at_phone_list,
            "isAtAll": False
        }
    }
    resp = requests.post(ding_webhook, json=data)
    resp.close()


if __name__ == '__main__':
    from svn_read_v1.ReadConfig import ReadConfig

    rc = ReadConfig()
    logs = read_svn_log(rc)

    author_set = set()
    for i in logs:
        author_set.add(i.get_author().strip())

    employee_dict = get_employee_dict()

    employee_set = set(employee_dict.keys())
    no_commit_set = employee_set - author_set

    no_commit_user_str = '## SVN日志管理提示 ##\n'
    at_phone_list = []
    if len(no_commit_set) == 0 or (len(no_commit_set) == 1 and checkout(list(no_commit_set)[0])):
        no_commit_user_str += '昨日SVN已全部提交，请诸位继续保持。'
    else:
        no_commit_user_str += '检测到以下同学：\n'
        for user in no_commit_set:
            if user == '' or user is None or checkout(user):
                continue
            no_commit_user_str = no_commit_user_str + '\t- ' + employee_dict[user]
            at_phone_list.append(employee_dict[user].split('@')[1].strip())

        no_commit_user_str += '\n昨日**未提交**SVN代码，请自行检查并提交。（如有问题请举手报告）'

    print(employee_set)
    print(author_set)
    print(no_commit_set)
    print(no_commit_user_str)

    send_ding_msg('testtest', no_commit_user_str, at_phone_list)
