import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import col
import sys
from datetime import timedelta
import datetime
import math
import re
import sys
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, avg
from pyspark.sql.functions import udf

# sc = pyspark.SparkContext()
# sqlContext = pyspark.sql.HiveContext(sc)

output_table = "svcdsim.wiki_people"
data = sc.wholeTextFiles("hdfs:///Data_w/*/*")


def get_title(content):
    content = content.encode('utf8').strip()
    title = ''
    try:
        if(content != ''):
            arr = content.split("\n",2)
            title = arr[1]
            actual_content = arr[2]
    except:
        title = ''
    return title


def get_content(content):
    content = content.encode('utf8').strip()
    actual_content = ''
    try:
        if(content!=''):
            arr = content.split("\n",2)
            actual_content = arr[2]
    except:
        actual_content = ''
    return actual_content


def check_if_person(content):
    content = content[:150]
    list1 = re.findall(r"[\d]{1,2} [ADFJMNOS]\w* [\d]{4}", content)
    list2 = re.findall(r"[ADFJMNOS]\w* [\d]{1,2}[,] [\d]{4}", content)
    if(len(list1)>0 or len(list2)>0):
        return True
    return False

# x = data.map(lambda x :(x[0],1))
pages = data.flatMap(lambda x :(x[1].split('</doc>'))).map(lambda x : (get_title(x),get_content(x))).\
filter(lambda x: ((len(x[0])!=0) or (len(x[1])!=0))).filter(lambda x : check_if_person(x[1]))


df = sqlContext.createDataFrame(pages, ["title", "content"])
df.write.mode("overwrite").saveAsTable(output_table)
