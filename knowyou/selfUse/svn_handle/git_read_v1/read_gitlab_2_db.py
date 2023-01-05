# -*- coding:utf-8 -*-

import requests, json, sqlite3, uuid
import datetime as dt
import os
import sys

dirname = os.path.split(os.path.realpath(sys.argv[0]))[0]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
}

# gitlab地址
git_url = 'http://192.168.2.212/'
# gitlab的token
git_token = '1KxiDLjQME-W9uhAQHDm'

session = requests.Session()
headers['PRIVATE-TOKEN'] = git_token
session.headers = headers
git_login = session.get(git_url, headers=headers)


# 获取当前账号有权限的全部工程列表
def gitlab_projects():
    print("正在获取gitlab上工程数量...")
    projects_api = git_url + '/api/v4/projects?simple=yes&per_page=20'
    projects_headers = session.head(projects_api).headers
    projects_num = int(projects_headers['X-Total'])
    projects_pages = int(projects_headers['X-Total-Pages'])
    # print(projects_headers)
    print("工程总数：", projects_num)
    cursor = conn.cursor()
    cursor.execute(
        'create table if not exists gitlab_projects(id varchar(8),name varchar(128),desc varchar(256),path varchar(128),create_at varchar(64),default_branch varchar(64),branch_num varchar(16),ssh_url_to_repo varchar(128),web_url varchar(128),PRIMARY KEY ("id"))')
    cursor.execute('delete from gitlab_projects')
    for i in range(projects_pages):
        pages = i + 1
        projects_url = projects_api + '&page=' + str(pages)
        projects = session.get(projects_url).text
        # print(projects)
        projects_json = json.loads(projects)
        for project_json in projects_json:
            project_id = project_json['id']
            project_name = project_json['name']
            project_desc = project_json['description']
            project_path = project_json['path_with_namespace']
            project_create_at = project_json['created_at']
            # project_create_at=project_create_at[0:19].replace('T',' ')
            project_default_branch = project_json['default_branch']
            project_ssh_url_to_repo = project_json['ssh_url_to_repo']
            project_web_url = project_json['web_url']
            cursor.execute('insert into gitlab_projects values(?,?,?,?,?,?,?,?,?)', (
                project_id, project_name, project_desc, project_path, project_create_at, project_default_branch, 'null',
                project_ssh_url_to_repo, project_web_url))
    cursor.close()
    conn.commit()
    print("工程获取完成")


# 获取工程分支
def gitlab_project_branchs(project_list):
    print("获取工程分支信息...")
    cursor = conn.cursor()
    cursor.execute(
        'create table if not exists gitlab_project_branch(id varchar(8),name varchar(128),branch_name varchar(128))')
    cursor.execute('delete from gitlab_project_branch')
    project_list = project_list
    for project_info in project_list:
        project_id = project_info[0]
        project_name = project_info[1]

        # 自定义
        print('工程id是' + project_id + ':', '工程名是' + project_name)

        project_branchs_api = git_url + '/api/v4/projects/' + project_id + '/repository/branches?per_page=100'
        project_branchs = session.get(project_branchs_api).text

        # 自定义
        print(project_branchs)

        project_branchs_json = json.loads(project_branchs)
        project_branchs_num = len(project_branchs_json)

        # 自定义
        print(str(project_name) + '工程的分支数量是' + str(project_branchs_num))

        # print(project_branchs_num)
        for project_branchs in project_branchs_json:
            try:
                prject_branch = project_branchs['name']
            except Exception as e:
                print(e)
            # 自定义
            print(prject_branch)

            cursor.execute('insert into gitlab_project_branch values(?,?,?)', (project_id, project_name, prject_branch))
        cursor.execute('update gitlab_projects set branch_num=? where id=?', (project_branchs_num, project_id))
    cursor.close()
    conn.commit()
    print("分支信息获取完成")


# 增量获取所有工程所有分支的提交日志
def gitlab_project_commits(project_list):
    print("获取工程commit日志...")
    cursor = conn.cursor()
    cursor.execute(
        'create table if not exists gitlab_project_commits(xmlid varchar(64),id varchar(8),name varchar(128),branch_name varchar(128),title varchar(512),additions varchar(8),deletions varchar(8),create_at varchar(32),author_name varchar(32),author_email varchar(64),PRIMARY KEY ("xmlid"))')
    # cursor.execute('delete from gitlab_project_commits')
    cursor.close()
    project_list = project_list
    for project_info in project_list:
        project_id = project_info[0]
        project_name = project_info[1]
        # print(project_id,project_name)
        cursor = conn.cursor()
        cursor.execute('select branch_name from gitlab_project_branch where id=? and name=?',
                       (project_id, project_name))
        project_branch_list = cursor.fetchall()
        for project_branch in project_branch_list:
            project_branch_name = project_branch[0]
            cursor.execute('select max(create_at) from gitlab_project_commits where id=? and branch_name=?',
                           (project_id, project_branch_name))
            max_create_at = cursor.fetchall()[0][0]
            since = max_create_at
            if since:
                project_branchs_commit_api = git_url + '/api/v4/projects/' + project_id + '/repository/commits?per_page=50&ref_name=' + project_branch_name + '&since=' + since
            else:
                project_branchs_commit_api = git_url + '/api/v4/projects/' + project_id + '/repository/commits?per_page=50&ref_name=' + project_branch_name
            # print(project_branchs_commit_api)
            project_branchs_commit_headers = session.head(project_branchs_commit_api).headers
            projects_num = int(project_branchs_commit_headers['X-Total'])
            projects_pages = int(project_branchs_commit_headers['X-Total-Pages'])
            print("正在增量获取" + project_name + "的" + project_branch_name + "分支的" + str(projects_num) + "条commit日志")
            for i in range(projects_pages):
                page = i + 1
                project_branchs_commit_api_page = project_branchs_commit_api + '&page=' + str(page)
                # print(project_branchs_commit_api_page)
                project_branchs = session.get(project_branchs_commit_api_page).text
                project_branchs_commit_json = json.loads(project_branchs)
                for project_branch_commit_json in project_branchs_commit_json:
                    # print(project_branch_commit_json)
                    # id,name,branch_name,title ,create_at ,author_name,author_email
                    commit_add = 0
                    commit_del = 0
                    commit_id = project_branch_commit_json['id']
                    commit_title = project_branch_commit_json['title']
                    commit_create_at = project_branch_commit_json['created_at']
                    # commit_create_at=commit_create_at[0:19].replace('T',' ')
                    commit_author_name = project_branch_commit_json['author_name']
                    commit_author_email = project_branch_commit_json['author_email']
                    # start 获取每个commit的增加和删除行，太慢了，暂时注释掉
                    # project_branchs_commit_info_url=git_url+ '/api/v4/projects/' + project_id + '/repository/commits/'+commit_id
                    # project_branchs_commit_info_re=session.get(project_branchs_commit_info_url).text
                    # project_branchs_commit_info_json=json.loads(project_branchs_commit_info_re)
                    # commit_add=project_branchs_commit_info_json['stats']['additions']
                    # commit_del=project_branchs_commit_info_json['stats']['deletions']
                    # end
                    uuid_key = str(uuid.uuid1()).replace('-', '')
                    cursor.execute('insert into gitlab_project_commits values(?,?,?,?,?,?,?,?,?,?)', (
                        uuid_key, project_id, project_name, project_branch_name, commit_title, commit_add, commit_del,
                        commit_create_at, commit_author_name, commit_author_email))
        cursor.close()
        conn.commit()
    print("分支信息获取完成")


def get_employee_dict():
    file_name = "employee.dict"
    dict_path = os.path.join(dirname, file_name)

    employee_dict = {}
    with open(dict_path, encoding='utf-8') as f:
        for line in f:
            (email, employee) = line.split('|')
            employee_dict[email] = employee

    return employee_dict


def write_2_xls(user_list):
    import xlwt
    import xlrd
    from xlrd import XLRDError
    from xlutils.copy import copy as xl_copy

    file_directory = "no-commit-user-result"
    dict_path = os.path.join(dirname, file_directory)
    yesterday = (dt.datetime.now() - dt.timedelta(days=1))
    file_month = yesterday.strftime('%Y-%m.xls')
    file_day = yesterday.strftime('%Y-%m-%d')
    file_name = os.path.join(dict_path, file_month)
    flag = os.path.exists(file_name)

    if flag:
        rb = xlrd.open_workbook(file_name)
        book = xl_copy(rb)
        try:
            table = rb.sheet_by_name(file_day)
        except XLRDError:
            sheet = book.add_sheet(file_day, cell_overwrite_ok=True)
        else:
            sheet = book.get_sheet(file_day)
    else:
        book = xlwt.Workbook(encoding='utf-8')
        sheet = book.add_sheet(file_day, cell_overwrite_ok=True)

    i = 1
    for col in user_list:
        sheet.write(i, 0, col)
        i = i + 1

    book.save(file_name)


def send_ding_msg(title, content, at_phone_list):
    '''
curl 'https://oapi.dingtalk.com/robot/send?access_token=5d28b13ac0ae5a59e913204e629d4822bf3c64abdb826b971d5389d3815bea4e' \
-H 'Content-Type: application/json' \
-d '{"msgtype": "text","text": {"content":"svn test test"}}'

    :param title:
    :param content:
    :return:
    '''

    yanfazhongxin_webhook = "https://oapi.dingtalk.com/robot/send?access_token=da09440ae680b554a916ec31c0beb3bb4a9944f665e2aeffaa5b5215f1e780c6"

    data = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"{title}",
            "text": f"{content} \n"
        },
        "at": {
            "atMobiles": at_phone_list,
            "isAtAll": False
        }
    }
    resp = requests.post(yanfazhongxin_webhook, json=data)
    resp.close()


if __name__ == "__main__":
    conn = sqlite3.connect(os.path.join(dirname, 'gitlab.db'))
    cursor = conn.cursor()

    # 获取gitlab上的工程列表
    gitlab_projects()

    # 获取gitlab_projects的id和name
    cursor.execute('select id,name from gitlab_projects')
    project_list = cursor.fetchall()

    # 获取gitlab上每个工程的分支列表
    gitlab_project_branchs(project_list)

    # 增量获取gitlab上每个工程的分支的commit记录
    gitlab_project_commits(project_list)

    yesterday = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d') + '%'

    cursor.execute('select distinct author_email from gitlab_project_commits where create_at like ?', (yesterday,))

    authors = cursor.fetchall()

    conn.close()

    commit_set = set()
    for author in authors:
        commit_set.add(author[0])

    employee_dict = get_employee_dict()

    emp_set = set(employee_dict.keys())

    no_commit_set = emp_set - commit_set

    no_commit_user_str = '## Gitlab日志管理提示 ##\n'
    at_phone_list = []
    no_commit_list = []

    if len(no_commit_set) == 0:
        no_commit_user_str += '昨日Gitlab已全部提交。'
    else:
        no_commit_user_str += '检测到以下同学：\n'
        for user in no_commit_set:
            if user == '' or user is None:
                continue
            if user in ['568579055@qq.com', '453981759@qq.com', '1575660772@qq.com']:
                no_commit_user_str = no_commit_user_str + '\t- @' + employee_dict[user].split('@')[0] + '\n'
            else:
                no_commit_user_str = no_commit_user_str + '\t- @' + employee_dict[user].split('@')[1]
                at_phone_list.append(employee_dict[user].split('@')[1].strip())
            no_commit_list.append(employee_dict[user].split('@')[0].strip())
    no_commit_user_str += '\n\r 昨日**未提交**Gitlab代码，请自行检查并提交。'

    print(yesterday)
    print(commit_set)
    print(emp_set)
    print(no_commit_set)
    print(no_commit_user_str)
    print(no_commit_list)
    print(at_phone_list)

    # if 6 != dt.datetime.now().weekday() and 0 != dt.datetime.now().weekday():
    #     send_ding_msg('Git每日提醒', no_commit_user_str, at_phone_list)
    #     write_2_xls(no_commit_list)