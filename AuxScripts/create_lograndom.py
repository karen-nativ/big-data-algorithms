import math
from random import random

n = 10000
edge_prob = (math.log(n,2))/n

def edge_variable():
	return random() <= edge_prob

def print_lograndom():
    full_graph = ""
    for v1 in range(n):
        for v2 in range(v1+1, n):
            if edge_variable():
                full_graph += str(v1)+"\t"+str(v2)+"\n"
    f = open("src/main/resources/giraph/lograndom_graph.txt")
    f.write(full_graph)
    f.close()
