'''实现加减乘除及拓号优先级解析
用户输入 1 - 2 * ( (60-30 +(-40/5) * (9-2*5/3 + 7 /3*99/4*2998 +10 * 568/14 )) - (-4*3)/ (16-3*2) )
等类似公式后，必须自己解析里面的(),+,-,*,/符号和公式，
运算后得出结果，结果必须与真实的计算器所得出的结果一致'''

import re
user_input = "1-2*((60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14))-(-4*3)/(16-3*2))"


# pattern = re.compile(r'\(-?\d.+\d\)')
# re_result = re.findall(pattern, user_input)
# print(re_result[0])

# data = "(60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14))-(-4*3)/(16-3*2)"
# pattern = re.compile(r'(.*\))-?\+?\/?\*?(\(.*)')
# re_result = re.match(pattern, data)
# for line in re_result.groups():
#     print(line)

# data = '(60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14))'
# if data.startswith("(") and data.endswith(")"):
#     pattern = re.compile(r'\((.*)\)')
#     re_result = re.match(pattern, data)
#     print(re_result.groups()[0])

# data = "60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14)"
# pattern = re.compile(r'\(-?\d.*\d\)')
# re_result = re.findall(pattern, data)
# print(re_result)

# data = "(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14)"
# pattern = re.compile(r'(.*\))-?\+?\/?\*?(\(.*)')
# re_result = re.match(pattern, data)
#
# for line in re_result.groups():
#     print(line)


data = "(9-2*5/3+7/3*99/4*2998+10*568/14)"
if data.startswith("(") and data.endswith(")"):
    pattern = re.compile(r'\((.*)\)')
    re_result = re.match(pattern, data)
    # print(re_result.groups()[0])

    ##取调所有 括号了

    aaa = "9-2*5/3+3-7/3*99/4*2998+10*568/14-8*9+9/8"
    pattern = re.compile(r'\+|\-')
    re_result = re.split(pattern, aaa)
    print(re_result)

    for line in re_result:
        # print(line)
        ccc = re.findall("\*|\/", line)
        if ccc != []:
            print(line)
            