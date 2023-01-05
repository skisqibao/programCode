# -*- coding:utf-8 -*-
import gitlab
import datetime as dt

def read_gitlab(start, end):
    gl = gitlab.Gitlab('https://gitlab.knowyou.com.cn', private_token='jqWa8zrhVPU3vLDpyUgQ', keep_base_url=True)
    # gl = gitlab.Gitlab('https://gitlab.knowyou.com.cn', private_token='1KxiDLjQME-W9uhAQHDm', keep_base_url=True)
    # gl = gitlab.Gitlab('https://gitlab.knowyou.com.cn', private_token='PVUSmigHAG6seFCTNu4y', keep_base_url=True)
    gl.auth()

    # project = gl.projects.get('cmss/cdispatching_cut_services')
    # commits = project.commits.list(get_all=True, all=True)
    # for c in commits:
    #     print(c.id, c.committer_name, c.created_at, c.message)

    # list all the projects
    projects = gl.projects.list(get_all=True)
    commit_set = set()
    for project in projects:
        #print(project.web_url)
        commits = project.commits.list(since=start, until=end)
        for c in commits:
            print(c.committer_name, c.created_at, c.message, c.author_email)
            commit_set.add(c.committer_name)

    return commit_set

if __name__ == "__main__":
    yesterday = (dt.datetime.now() - dt.timedelta(days=1))
    start = yesterday.strftime('%Y-%m-%dT00:00:00')
    end = yesterday.strftime('%Y-%m-%dT23:59:59')

    commit_set = read_gitlab(start, end)

    print(commit_set)