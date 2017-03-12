from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def task7map(line):
    issue_date = line[1]
    issue_day = int(issue_date.split('-')[2])
    violation_code = line[2]
    if issue_day in [5,6,12,13,19,20,26,27]:
        return ((int(violation_code),(1,0)))
    else:
        return ((int(violation_code),(0,1)))

if __name__ == "__main__":
    #suppose: argv[1]:parking.csv argv[2]open.csv
    sc = SparkContext()
    parking_csv = sc.textFile(sys.argv[1],1)
    #open_csv = sc.textFile(sys.argv[2],1)
    parking_lines = parking_csv.mapPartitions(lambda x: reader(x))
    #open_lines = open_csv.mapPartitions(lambda x: reader(x))
    parking_lines.map(task7map).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).sortByKey()\
.map(lambda x: '%s\t%.2f,%.2f'%(x[0],x[1][0]/8.,x[1][1]/23.)).saveAsTextFile('task7.out')