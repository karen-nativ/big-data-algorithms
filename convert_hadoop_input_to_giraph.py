hadoop_string = """
Bushtyno is an urban-type settlement in Tiachiv Raion (district) of Zakarpattia Oblast (region) in western Ukraine. The town's population was 8,506 as of the 2001 Ukrainian Census. Current population: Population: 8,516 (2021 est.). Bushtyno is located in a small valley where the Tereblia and the Tisza rivers meet.

The settlement was first mentioned in 1373 as the village of Bushta. In 1910, the settlement belonged to the Máramaros County of the Kingdom of Hungary. At the time the settlement was known as Bustyaháza, a Hungarian variant of the name, and contained a total of 2,056 inhabitants, the majority of which were Ruthenians.

In 1930, the settlement's Jewish population was 1,042. In 1957, the settlement was granted the status of an urban-type settlement. In 1995, the settlement was renamed from Bushtyna to the current "Bushtyno."

"""

all_words = hadoop_string.split()
giraph_string = ""
for index in range(len(all_words)-1):
    giraph_string += all_words[index]+str(index)+"\t0.0\t"+all_words[index+1]+str(index+1)+"\t0.0\n"
f = open("/home/hduser/b649-hadoop/src/main/resources/giraph/input.txt", "w")
f.write(giraph_string)
f.close()
