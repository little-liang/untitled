'''
[0, 1, 2, 3]
[0, 1, 2, 3]
[0, 1, 2, 3]
[0, 1, 2, 3]
----------------
[0, 0, 0, 0]
[1, 1, 1, 1]
[2, 2, 2, 2]
[3, 3, 3, 3]

'''



data = [[raw for raw in range(4)] for col in range(4)]
for line in data:
    print(line)
for r_index,row in enumerate(data):
    for c_index in range(r_index,len(row)):
        tmp = data[c_index][r_index]
        data[c_index][r_index] = row[c_index]
        data[r_index][c_index] = tmp
    print("---------")
for line in data:
    print(line)


