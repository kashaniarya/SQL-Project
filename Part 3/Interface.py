#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    '''
    takes a file system path that contains the ratings.dat file as input. 
    then load the ratings.dat content into a table (saved in PostgreSQL) named Ratings 
    that has the following schema:
    UserID(int) – MovieID(int) – Rating(float)
    '''
    cursor = openconnection.cursor()
    cursor.execute("CREATE TABLE " + ratingstablename + " (Row SERIAL PRIMARY KEY, UserID INT, D1 VARCHAR(10), MovieID INT , D2 VARCHAR(10), Rating REAL, D3 VARCHAR(10), D4 BIGINT)")
    inputFile = open(ratingsfilepath, 'r')
    cursor.copy_from(inputFile, ratingstablename, sep=':', columns=('UserID', 'D1', 'MovieID', 'D2', 'Rating', 'D3', 'D4'))
    cursor.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN D1, DROP COLUMN D2, DROP COLUMN D3, DROP COLUMN D4")
    cursor.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    '''
    takes as input: (1) the Ratings table stored in PostgreSQL 
    and (2) an integer value N; that represents the number of partitions. 
    then generates N horizontal fragments of the Ratings table and stores them in PostgreSQL. 
    fThe algorithm should partition the ratings table based on N uniform ranges of the Rating attribute.
    '''
    cursor = openconnection.cursor()
    ratingRange = 5.0 / numberofpartitions
    partitonName = 0
    ratingStart = 0
    while ratingStart < 5.0:
        if ratingStart == 0:
            cursor.execute("CREATE TABLE range_part" + str(partitonName) + " AS SELECT UserID, MovieID, Rating FROM " + ratingstablename + " WHERE Rating <=" + str(ratingStart + ratingRange) + ";")
        else:
            cursor.execute("CREATE TABLE range_part" + str(partitonName) + " AS SELECT UserID, MovieID, Rating FROM " + ratingstablename + " WHERE Rating >" + str(ratingStart) + " AND Rating <=" + str(ratingStart + ratingRange) + ";")
        partitonName = partitonName + 1
        ratingStart = ratingStart + ratingRange
    cursor.close()

    
def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    '''
    takes as input: (1) the Ratings table stored in PostgreSQL 
    and (2) an integer value N; that represents the number of partitions. 
    The function then generates N horizontal fragments of the Ratings table and stores them in PostgreSQL. 
    The algorithm should partition the ratings table using the round robin partitioning approach (explained in class).
    '''
    cursor = openconnection.cursor()
    list_partitions = list(range(numberofpartitions))
    cursor.execute("create table meta_rrobin_part_info (partition_number INT, numberofpartitions INT)")
    last=-1
    for partion in list_partitions:
        cursor.execute("DROP TABLE if EXISTS rrobin_part" + str(partion))
        cursor.execute("CREATE TABLE rrobin_part" + str(partion) + " as SELECT UserID, MovieID, Rating FROM (SELECT UserID, MovieID, Rating, row_number() over() as row_num from " + str(ratingstablename) + ") a where (a.row_num -1 + " + str(numberofpartitions) + ")% " + str(numberofpartitions) + " = " + str(partion))
        last=partion
    cursor.execute("TRUNCATE TABLE meta_rrobin_part_info") 
    cursor.execute("INSERT INTO meta_rrobin_part_info VALUES (" + str(last) + "," + str(numberofpartitions) + ")")
    cursor.close()

    
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    '''
    takes as input: (1) Ratings table stored in PostgreSQL, 
    (2) UserID, (3) MovieID, (4) Rating. 
    then inserts a new tuple to the Ratings table 
    and the right fragment based on the round robin approach.
    '''
    cursor = openconnection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name like 'rrobin_part%'")
    numberOfPart = 0
    for row in cursor:
        numberOfPart = numberOfPart + 1

    endingPartNumber = 0
    maxRowPart = 0
    for tableNumber in range(numberOfPart):
        cursor.execute("SELECT COUNT (*) FROM rrobin_part" + str(tableNumber) + ";")
        currPartCount = cursor.fetchone()[0]

        if currPartCount >= maxRowPart:
            endingPartNumber = tableNumber
            maxRowPart = currPartCount

    currPartition = (endingPartNumber + 1) % numberOfPart
    cursor.execute("INSERT INTO rrobin_part" + str(currPartition) + " (UserID ,MovieID, Rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ")")
    cursor.close()

    
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    '''
    takes as input: (1) Ratings table stored in Postgresql 
    (2) UserID, (3) MovieID, (4) Rating. 
    then inserts a new tuple to the Ratings table 
    and the correct fragment (of the partitioned ratings table) based upon the Rating value.
    '''
    cursor = openconnection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name like 'range_part%'")
    numberOfPart = 0
    for row in cursor:
        numberOfPart = numberOfPart + 1
    ratingRange = 5.0 / numberOfPart
    partitionnumber = 0
    ratingEnd = ratingRange

    while ratingEnd <= 5.0:
        if rating <= ratingEnd:
            break
        ratingEnd = ratingEnd + ratingRange
        partitionnumber = partitionnumber + 1

    cursor.execute("INSERT INTO range_part" + str(partitionnumber) + " (UserID, MovieID, Rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ")")
    cursor.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()