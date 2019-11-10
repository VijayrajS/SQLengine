import sys
import csv


class Table:
    def __init__(self, name, columns):
        self.name = name
        self.cols = columns
        self.data = []

    def fill_data(self):
        with open(self.name+'.csv') as csvfile:
            datareader = csv.reader(csvfile, delimiter=',')

            for row in datareader:
                self.data.append([int(u) for u in row])

    def push_data(self, newdata):
        self.data = newdata


class Database:
    def __init__(self, metadata):
        self.tables = {}
        self.m_file = metadata

    def fillDB(self):
        with open(self.m_file, "r") as meta:
            extract_name = False

            temp_name = ''
            temp_cols = []

            for line in meta:
                line = line.strip()
                if extract_name == True:
                    temp_name = line.lower()
                    extract_name = False
                    continue

                if line == '<begin_table>':
                    extract_name = True
                    continue

                if line == '<end_table>':
                    self.tables[temp_name] = Table(temp_name, temp_cols)
                    self.tables[temp_name].fill_data()
                    temp_name = ''
                    temp_cols = []
                    continue

                temp_cols.append(line.lower())
