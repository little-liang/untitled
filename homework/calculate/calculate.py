'''实现加减乘除及拓号优先级解析
用户输入 1 - 2 * ( (60-30 +(-40/5) * (9-2*5/3 + 7 /3*99/4*2998 +10 * 568/14 )) - (-4*3)/ (16-3*2) )
等类似公式后，必须自己解析里面的(),+,-,*,/符号和公式，
运算后得出结果，结果必须与真实的计算器所得出的结果一致'''

import re
def liukuohao(user_input):
    pattern = re.compile(r'\(-?\d.+\d\)')
    re_result = re.findall(pattern, user_input)
    return re_result

def chaikuohao(user_input):
    user_input_list = []
    pattern = re.compile(r'(.*\))-?\+?(\(.*)')
    re_result = re.match(pattern, user_input)
    for line in re_result.groups():
        user_input_list.append(line)
    return user_input_list

def quliangbiankuohao(user_input):
    if user_input.startswith("(") and user_input.endswith(")"):
        pattern = re.compile(r'\((.*)\)')
        re_result = re.match(pattern, user_input)
        return re_result.groups()[0]


def chaizuihoudekuohao(user_input):
    user_input_list = []
    pattern = re.compile(r'(.*\))-?\+?\/?\*?(\(.*)')
    re_result = re.match(pattern, user_input)
    for line in re_result.groups():
        user_input_list.append(line)

    return user_input_list

def zhijiechai(user_input):

    #去括号两边的重复括号，去掉后放到tmp——List中
    tmp_list = []

    for line in user_input:
        #每一次判断，判断括号去掉后的情况
        flag = False
        pattern = re.compile(r'(.*\))-?\+?\/?\*?(\(.*)')
        re_result = re.match(pattern, line)

        #判断能不能安全的去两边的括号，按照去掉括号后，左括号与右括号是否相等
        for line2 in re_result.groups():
            pattern = re.compile(r"\(")
            pattern2 = re.compile(r"\)")
            re_result = re.findall(pattern, line2)
            re_result2 = re.findall(pattern2, line2)
            if re_result.count("(") == re_result2.count(")"):
                pass
            else:
                flag = True

        #去掉括号，并且放入tmp——list
        if flag:
            pattern = re.compile(r'\((.*)\)')
            re_result = re.match(pattern, line)
            tmp_list.append(re_result.groups()[0])
        else:
            tmp_list.append(line)
    return tmp_list

def kaishile(user_input):
    pattern = re.compile(r"^\(")
    re_result = re.match(pattern, user_input)
    if re_result:
        print(1)
    else:
        re_result = liukuohao(user_input)
        return "".join(re_result)

def kaishile2(user_input):
    user_input = "".join(user_input)
    re_result = chaikuohao(user_input)
    return re_result



if __name__ == '__main__':
    user_input = "1-2*((60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14))-(-4*3)/(16-3*2))"
    print(user_input)

    #第一步，如果开头不是括号
    re_result = kaishile(user_input)
    print(re_result)

    #2，开头是括号
    re_result = kaishile2(re_result)
    # print(re_result)

    #3这时候，肯定因式分解了

    re_result = zhijiechai(re_result)
    # print(re_result)

    for l1 in re_result:
        print(l1)
        l2 = kaishile(l1)
        print(l2)
        break

        ##最开头出现问题1-2*((60-30+(-40/5)*(9-2*5/3+7/3*99/4*2998+10*568/14))-(-4*3)/(16-3*2))
        ##应该是1自己一组，-号自己一组，2*（）再一组，这里没弄对，运算符号还没保存
