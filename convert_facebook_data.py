from collections import defaultdict
f = open("src/main/resources/giraph/facebook_graph.txt", "r")
input_lines = f.readlines()
f.close()

edge_dict = defaultdict(list)
for line in input_lines:
    edge = line.split()
    edge_dict[edge[1]].append([int(edge[0]),0])
    edge_dict[edge[0]].append([int(edge[1]),0])

new_file = ""
for key in edge_dict:
    new_file += "[" + key + ",0,"+ str(edge_dict[key]).replace(' ', '') + "]\n"
    
f2 = open("src/main/resources/giraph/triangle_input.txt", "w")
f2.write(new_file)
f2.close()
