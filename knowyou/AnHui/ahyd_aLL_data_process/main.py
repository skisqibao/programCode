#!/usr/bin/env python

import xlrd


def print_hi(file):
    bk = xlrd.open_workbook(file)

    sh = bk.sheet_by_index(0)

    nrows = sh.nrows
    ncols = sh.ncols

    print("nrows:" + nrows + "ncols:" + ncols)


if __name__ == '__main__':
    file = 'test_data.xlsx'
    print_hi(file)
