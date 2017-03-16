
import sys
with open('/tmp/test01.txt', 'a', encoding='utf-8') as file1:
    data = "这是第[%s]次运行\n"
    print(data)
    file1.write(data)