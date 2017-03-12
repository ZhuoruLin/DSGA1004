from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    #suppose: argv[1]:parking.csv argv[2]open.csv
    sc = SparkContext()
    parking_csv = sc.textFile(sys.argv[1],1)
    open_csv = sc.textFile(sys.argv[2],1)
    parking_lines = parking_csv.mapPartitions(lambda x: reader(x))
    open_lines = open_csv.mapPartitions(lambda x: reader(x))
    in_parking = parking_lines.map(lambda x: (x[0],1))
    in_open = open_lines.map(lambda x: (x[0],-1))
    combined = in_parking.union(in_open)
    reduced = combined.reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]==1)
    def extract_parking_info(parking_line):
        return (parking_line[0],parking_line[14]+', '+parking_line[6]+', '+parking_line[2]+', '+parking_line[1])
    parking_info = parking_lines.map(extract_parking_info)
    parking_info.join(reduced).map(lambda x: (x[0],x[1][0])).sortByKey().map(lambda x:'%s\t%s'%(x[0],x[1])).saveAsTextFile("task1.out")