from collections import defaultdict
f = open("src/main/resources/giraph/lograndom_graph.txt", "r")
input_lines = f.readlines()
f.close()

edge_dict = defaultdict(list)
for line in input_lines:
    edge = line.split()
    if edge[1] not in edge_dict[edge[0]]:
        edge_dict[edge[0]].append([int(edge[1])])

new_file = ""
for key in edge_dict:
    for val in edge_dict[key]:
        new_file += str(key) + "\t"+ str(val[0]) + "\n"
    
f2 = open("src/main/resources/giraph/facebook_graph_hadoop.txt", "w")
f2.write(new_file)
f2.close()
