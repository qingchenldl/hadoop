import os

def file_name(file_dir):
	L = []
	for root,dirs,files in os.walk(file_dir):
		for file in files:
			filename = os.path.split(file)[1]
			L.append(os.path.join(root, filename))
	return L

files = file_name('class')

def abcde(scorelist):
	A=B=C=D=E = 0
	for s in scorelist:
		score = int(s)
		if score>=90 and score<=100:
			A += 1
		elif score>=80 and score<90:
			B += 1
		elif score>=70 and score<80:
			C += 1
		elif score>=60 and score<70:
			D += 1
		else:
			E += 1
	return A,B,C,D,E

def min_max(scorelist):
	maxval = -1
	minval = 150
	for s in scorelist:
		score = int(s)
		if(score > maxval):
			maxval = score
		if(score < minval):
			minval = score
	return minval, maxval

scores = []
for f in files:
	claScore = []
	fp = open(f,'r')
	try:
		while True:
			text_line = fp.readline()
			if text_line:
				content = text_line
				s = content.split(' ')[2]
				scores.append(s)
				claScore.append(s)
			else:
				break	
	finally:
		fp.close()
	classAvg = 0
	for i in claScore:
		classAvg = classAvg + int(i)
	classAvg = classAvg / len(claScore)
	A,B,C,D,E = abcde(claScore)
	minval,maxval = min_max(claScore)
	print("{0} Avgrage is: {1}".format(f,classAvg))
	print("minval: {0},  maxval: {1}".format(minval,maxval))
	print("A num: {0}".format(A))
	print("B num: {0}".format(B))
	print("C num: {0}".format(C))
	print("D num: {0}".format(D))
	print("E num: {0}".format(E))
	print("--------------------------------")

sum = 0
for s in scores:
	sum += int(s)
A,B,C,D,E = abcde(scores)
minval,maxval = min_max(scores)
print("All Average Score: {0}".format(sum/len(scores)))
print("minval: {0},  maxval: {1}".format(minval,maxval))
print("A num: {0}".format(A))
print("B num: {0}".format(B))
print("C num: {0}".format(C))
print("D num: {0}".format(D))
print("E num: {0}".format(E))

avgtest = 0 
s = [40.8,44.5,59.1,55.9,52.05,54.85,43.6,48.8,50.5,42.55,56.95,59.8,47.4,35.25,41.0,46.7,38.5,56.8,53.3,33.85]
for i in s:
	avgtest = avgtest + i
avgtest = avgtest / len(s)
print(avgtest)