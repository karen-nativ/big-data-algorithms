
f = open("/home/hduser/b649-hadoop/hadoop_input.txt","r")
all_text = f.read()
f.close()
all_words = all_text.split()
giraph_string = ""
for index in range(len(all_words)-1):
    giraph_string += all_words[index]+str(index)+"\t0.0\t"+all_words[index+1]+str(index+1)+"\t0.0\n"
f2 = open("/home/hduser/b649-hadoop/src/main/resources/giraph/input.txt", "w")
f2.write(giraph_string)
f2.close()
