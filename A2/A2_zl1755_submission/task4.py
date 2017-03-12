from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    #suppose: argv[1]:parking.csv argv[2]open.csv
    sc = SparkContext()
    parking_csv = sc.textFile(sys.argv[1],1)
    #open_csv = sc.textFile(sys.argv[2],1)
    parking_lines = parking_csv.mapPartitions(lambda x: reader(x))
    #open_lines = open_csv.mapPartitions(lambda x: reader(x))
    def task4map(line):
    #If new york return ('NY',1)
    #else return ('Other',1)
        if line[16] =='NY':
            return ('NY',1)
        else:
            return ('Other',1)
    parking_lines.map(task4map).reduceByKey(lambda x,y:x+y).map(lambda x:'%s\t%s'%(x[0],x[1]))\
    .saveAsTextFile('task4.out')