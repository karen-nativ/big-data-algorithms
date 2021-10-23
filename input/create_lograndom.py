import math
from random import random

n = 1024
edge_prob = (math.log(n,2))/n

def edge_variable():
	return random() <= edge_prob

def print_lograndom():
	for v1 in range(n):
		for v2 in range(v1+1, n):
			if edge_variable():
				print(str(v1)+"	"+str(v2))

print_lograndom()
