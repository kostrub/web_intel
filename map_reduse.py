import mincemeat
import sys
import glob
import os
from stopwords import allStopWords

badsym="&?!,.-:'[]()"

def filt(line):
	temp_list=line.split("::")
	authors=temp_list[1:len(temp_list)-1]
	title=str(temp_list[-1:]).translate(None,badsym).strip("\\n").lower().replace(" a ","")
	words_list=[]
	ret=()
	for word in title.split(" "):
		if word not in allStopWords:
		 	words_list.append(word)
	for author in authors:
		ret=ret+(author.strip(":"), words_list,)
	return ret

pwd = os.path.dirname(__file__)

try:
	files=glob.glob(pwd+"./hw3data/*")
except Exception, e:
	print e
if len(files)==0:
	print "Cant't find data files in hw3data directory"
	exit()
data=[]
for File in files:
	for line in open(File):
	 data.append(filt(line))
datasource = dict(enumerate(data))

def mapfn(k, v):
    for w in v[1]:
        yield str(v[0]+":"+w),1

def reducefn(k, vs):
    result = sum(vs)
    return result

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="changeme")

for t in sorted(results.viewitems(),cmp=lambda x,y: cmp(x[1], y[1]),reverse=True):
	print t[0].split(":")[0],\
			t[0].split(":")[1],\
				t[1]
