# -*- coding: utf-8 -*-

import os
import configparser

# Read the configuration file
class ReadConfig:
    def __init__(self, filepath = None):

        if filepath:
            configpath = filepath
        else:
            dirname = os.path.split(os.path.realpath(__file__))[0]
            configpath = os.path.join(dirname, 'config.ini')

        self.config = configparser.ConfigParser()
        self.config.read(configpath, encoding = 'utf-8')

        self.pymysql_uri = self.config['database_uri']['pymysql_uri']

        self.authorization_url = self.config['request_url']['authorization_url']

        self.query_url = self.config['request_url']['query_url']


if __name__ == '__main__':
    rc = ReadConfig()
    print(rc.database_uri)
    print(rc.authorization_url)
    print(rc.query_url)
