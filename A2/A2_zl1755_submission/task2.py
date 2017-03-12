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
    violations_count = parking_lines.map(lambda x: (int(x[2]),1)).reduceByKey(lambda x,y:x+y)\
.sortBy(keyfunc = lambda x:x[0]).map(lambda x: '%s\t%s'%(x[0],x[1])).saveAsTextFile('task2.out')