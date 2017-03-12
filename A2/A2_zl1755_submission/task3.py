from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    #suppose: argv[1]:parking.csv argv[2]open.csv
    sc = SparkContext()
    #parking_csv = sc.textFile(sys.argv[1],1)
    open_csv = sc.textFile(sys.argv[1],1)
    #parking_lines = parking_csv.mapPartitions(lambda x: reader(x))
    open_lines = open_csv.mapPartitions(lambda x: reader(x))
    open_lines.map(lambda x : (x[2],(float(x[12]),1))).sortByKey()\
.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).sortByKey()\
.map(lambda x: '%s\t%.2f, %.2f'%(x[0],x[1][0],x[1][0]/x[1][1])).saveAsTextFile('task3.out')