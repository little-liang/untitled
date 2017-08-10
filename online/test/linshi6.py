import pickle
def BinSearch(array, key, low, high, i):
    mid = int((low+high)/2)
    if key == array[mid]:  # 若找到
        print('找了多少次', i)
        return mid
        # return array[mid]

    if low > high:
        print(i)
        return False

    if key < array[mid]:
        i += 1
        return BinSearch(array, key, low, mid-1, i)  #递归

    if key > array[mid]:
        i += 1
        return BinSearch(array, key, mid+1, high, i)






if __name__ == "__main__":

    i = 1
    # a = ['劳动', '劳动力', '劳动合同', '劳动成果', '劳动节', '劳工', '劳斯', '劳斯接', '劳斯海', '劳累', '势', '势不可挡', '势力', '势在必行', '势头']

    with open('word_list.pk1', 'rb') as f:
        a = pickle.load(f)
        print(a)

        b = pickle.load(f)
        print(b)


        for line in b:
            i = 1
            index = BinSearch(a, line, 0, len(a) - 1, i)
            print(index, a[index])
            break


