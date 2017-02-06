import re

# user_input = "1-2*((60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14))-(-4*3)/(16-3*2))"
# pattern = re.compile(r'\(-?\d.+\d\)')
# re_result = re.findall(pattern, user_input)
# print("".join(re_result))
#
#
# user_input = "(60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14))-(-4*3)/(16-3*2)"
# user_input_list = []
# pattern = re.compile(r'(.*\))-?\+?(\(.*)')
# re_result = re.match(pattern, user_input)
# for line in re_result.groups():
#     user_input_list.append(line)
# print(user_input_list)


# user_input = "(60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14))"
#
if user_input.startswith("(") and user_input.endswith(")"):
    pattern = re.compile(r'\((.*)\)')
    re_result = re.match(pattern, user_input)
    print(re_result.groups()[0])

# data = "60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14)"
# pattern = re.compile(r'\(-?\d.*\d\)')
# re_result = re.findall(pattern, data)
# print(re_result)

# data = "(60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14))"
# pattern = re.compile(r'(.*\))-?\+?\/?\*?(\(.*)')
# re_result = re.match(pattern, data)
#
# for line in re_result.groups():
#     print(line)


# data = "(9-2*5/3+7/3*99/4*2998+10*568/14)"
# if data.startswith("(") and data.endswith(")"):
#     pattern = re.compile(r'\((.*)\)')
#     re_result = re.match(pattern, data)
    # print(re_result.groups()[0])

    ##取调所有 括号了

    # aaa = "9-2*5/3+3-7/3*99/4*2998+10*568/14-8*9+9/8"
    # pattern = re.compile(r'\+|\-')
    # re_result = re.split(pattern, aaa)
    # print(re_result)
    #
    # for line in re_result:
    #     # print(line)
    #     ccc = re.findall("\*|\/", line)
    #     if ccc != []:
    #         print(line)

user_input = list(['(60-30+(-40/5)', '(9-2*5/3+7/3*99/4*2998+10*568/14))'])

for line in user_input:
    print(line)
    pattern = re.compile(r"\(")
    pattern2 = re.compile(r"\)")
    re_result = re.findall(pattern, line)
    re_result2 = re.findall(pattern2, line)
    print(re_result.count("("))
    print(re_result2.count(")"))

    break