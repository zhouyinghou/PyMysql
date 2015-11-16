#! /usr/bin/env python
#-*- coding: utf-8

import xlsxwriter


def decode_string(string):
    ''' '''
    return string.decode('utf-8')


def write_excel(columns, data_set, filename, sheet_name = 'Sheet1'):
    ''' '''
    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet(sheet_name)

    col = 0
    row = 0
    for column in columns:
        column = decode_string(column)
        worksheet.write(row, col, column)
        col += 1

    row += 1
    for line in data_set:
        col = 0
        for item in line:
            if type(item) in (bytearray, str):
                item = str(item)
                item = decode_string(item)
            worksheet.write(row, col, item)
            col += 1
        row += 1

    workbook.close()


if __name__ == "__main__":
    columns = "abcdefghij"
    data_set = [[j for j in range(10)] for i in range(10)]
    write_excel(columns, data_set, "test.xlsx")
