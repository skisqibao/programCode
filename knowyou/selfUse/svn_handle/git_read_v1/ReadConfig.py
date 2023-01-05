# -*- coding: utf-8 -*-

import os
import sys
import configparser


class ReadConfig:
    def __init__(self, filepath=None):
        if filepath:
            self.config_path = filepath
        else:
            dirname = os.path.split(os.path.realpath(sys.argv[0]))[0]
            config_name = "config.ini"
            self.config_path = os.path.join(dirname, config_name)

        self.config = configparser.ConfigParser()
        self.config.read(self.config_path, encoding="utf-8")

        self.svn_path = self.config['svn']['svn_path']
        self.start_date = self.config['svn']['start_date']
        self.end_date = self.config['svn']['end_date']
        self.username = self.config['svn']['username']
        self.password = self.config['svn']['password']

    def set_svn_path(self, svn_path):
        self.config.set('svn', 'svn_path', svn_path)
        self.config.write(open(self.config_path, 'w'))

    def set_start_date(self, start_date):
        self.config.set('svn', 'start_date', start_date)
        self.config.write(open(self.config_path, 'w'))

    def set_end_date(self, end_date):
        self.config.set('svn', 'end_date', end_date)
        self.config.write(open(self.config_path, 'w'))

    def set_username(self, username):
        self.config.set('svn', 'username', username)
        self.config.write(open(self.config_path, 'w'))

    def set_password(self, password):
        self.config.set('svn', 'password', password)
        self.config.write(open(self.config_path, 'w'))

    def __str__(self):
        str = ("svn_path : %s\nstart_date : %s\nend_date : %s\n") % (self.svn_path, self.start_date, self.end_date)

        return str


if __name__ == '__main__':
    rc = ReadConfig()

    rc.set_username('songkkkk')
