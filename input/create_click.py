# characters = [chr(i+ord('a')) for i in range(ord('z')-ord('a'))]
for i1 in range(ord('a'), ord('z')+1):
	for i2 in range(i1+1, ord('z')+1):
		print(chr(i1) + "	" + chr(i2))