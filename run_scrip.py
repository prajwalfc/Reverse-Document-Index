#!/usr/bin/env python
# coding: utf-8

#!/usr/bin/env python
# coding: utf-8


import os
cmd = "spark-submit reverse_index.py"

returned_value = os.system(cmd)  # returns the exit code in unix
print("----------",returned_value,"------------------")
os.system("chmod 777 -R output")
if returned_value == 0:
    cmd = "cat output/dict/part-0000? >>output/temp"
    os.system(cmd)
    cmd = "sed 's/.//;s/,/  /;s/.$//' output/temp >> output/word_id"
    os.system(cmd)
    cmd = "cat output/word_index/part-0000? >>output/id_index"
    os.system(cmd)
    os.system("rm -r output/dict/ output/word_index/")
    os.system("rm output/temp")