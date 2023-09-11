#!/usr/bin/python3
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    retval = []
    cursor = openconnection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'roundrobinratingspart%'")
    partions = cursor.fetchall()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rangeratingspart%'")
    partions2 = cursor.fetchall()
    for partion in partions:
        cursor.execute("SELECT * FROM {}".format(partion[0]))
        results = cursor.fetchall()
        for result in results:
            if (result[2] <= ratingMaxValue and result[2] >= ratingMinValue):
                retval.append((partion[0], result[0], result[1], result[2]))
    for partion in partions2:
        cursor.execute("SELECT * FROM {}".format(partion[0]))
        results = cursor.fetchall()
        for result in results:
            if (result[2] <= ratingMaxValue and result[2] >= ratingMinValue):
                retval.append((partion[0], result[0], result[1], result[2]))
    writeToFile('RangeQueryOut.txt', retval)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    retval = []
    cursor = openconnection.cursor()
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    cursor.execute(query)
    results = cursor.fetchall()
    for result in results:
        if not (result[0] == 'rangeratingsmetadata' or result[0] == 'roundrobinratingsmetadata'):
            query = "SELECT * FROM {} WHERE Rating = {}".format(result[0], ratingValue)
            cursor.execute(query)
            values = cursor.fetchall()
            for value in values:
                retval.append((result[0], value[0], value[1], value[2]))
    writeToFile('PointQueryOut.txt', retval)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()