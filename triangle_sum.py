import sys

f = open(sys.argv[1], "r")
input_lines = f.readlines()
f.close()
total = 0.0
for line in input_lines:
    total += float(line.split()[1])
print(total)
