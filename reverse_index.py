#!/usr/bin/env python
# coding: utf-8



from pyspark import SparkContext

def functionName (x):
    import re
    for a,b in x:
        file_name = int(a.split("/")[-1])
        body_set = re.split(r'\W+',b)
        for word in body_set:
            if word.rstrip()!="":
                yield(word.strip().lower(),(file_name,))
        

if __name__=='__main__':
    from operator import itemgetter, attrgetter
    sc = SparkContext()
    rdd =  sc.wholeTextFiles("data/*")
    word_location_index = rdd.mapPartitions(functionName).reduceByKey(lambda x,y:tuple(set(x+y))).zipWithIndex()
    word_code = word_location_index.map(lambda x: (x[0][0],x[1]))
    code_location = word_location_index.map(lambda x: (x[1],x[0][1]))
    word_code.saveAsTextFile("output/dict")
    code_location.saveAsTextFile("output/word_index")






