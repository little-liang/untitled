import re
#
# data = "hello,dbbbbbbbfnamebnbnname,hello23423"
# r = re.match("hello", data)
# print(r.group())
# r = re.match("(h\w+).*(\d)$", data)
# print(r.groups())
# print(r.groupdict())
#
# print("##################")
# r =re.search("hello", data)
# print(r.group())
# print(r.groupdict())

# origin = "hello alex bcd abcd lge acd 19"
# r = re.findall("bcd", origin)
# print(r)

origin = "hello 1alex 1*&%*&^*ggg 1bcd abcd lge acd 19"
r = re.split("1", origin,maxsplit=2)
print(r)


