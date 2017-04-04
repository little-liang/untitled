import re

def operator_update(formula):
   # 对formula公式进行 去除空字符，更新运算符处理
    formula = formula.replace(" ", "")  # 去除空字符
    formula=formula.replace("+-","-")
    formula=formula.replace("--", "+")
    return formula


def calc_muldiv(formula_list):
    '''
    计算公式里面的乘除
    :param formula: 列表
    :return:
    '''
    for index,element in enumerate(formula_list):
        if "*" in element or "/" in element:
            operators=re.findall("[*/]",element)
            calc_list=re.split("[*/]",element)
            num = None
            print(calc_list)
            for i,e in enumerate(calc_list):
                if num:
                    if operators[i-1] == "*":
                        num *= float(e)
                    elif operators[i-1] == "/":
                        num /= float(e)
                else:
                    num = float(e)
            formula_list[index] = num
    return formula_list

def calc_plumin(operators,num_list):
    '''
    计算列表数字的加减
    :param operators: 运算符列表
    :param num_list: 进行运算的数字列表
    :return: 返回计算结果
    '''
    num = None
    for i, e in enumerate(num_list):
        if num:
            if operators[i - 1] == "+":
                num += float(e)
            elif operators[i - 1] == "-":
                num -= float(e)
        else:
            num = float(e)
    return num


def merge(plus_minus_operator,multiply_divide_list):
    '''
    把列表中这样的形式'2*' '-3*' '5/3*' '4/2'合并到一块
    :param formula_list:
    :return:
    '''
    for index, element in enumerate(multiply_divide_list):
        if element.endswith("*") or element.endswith("/"):
            multiply_divide_list[index] = element + plus_minus_operator[index] + multiply_divide_list[index+1]
            del multiply_divide_list[index+1]
            del plus_minus_operator[index]
            return merge(plus_minus_operator,multiply_divide_list)
    return plus_minus_operator,multiply_divide_list


def bracket_calc(formula):
    '''
    对括号最内层的formula公式进行计算
    :param formula:
    :return:
    '''
    formula = re.sub("[()]", "", formula)  # 去除两边的（）
    formula = operator_update(formula)
    plus_minus_operator = re.findall("[+-]",formula)       #列表 '+' '-' 运算符
    multiply_divide_list = re.split("[+-]",formula)        #列表 有'*' '/'
    if multiply_divide_list[0] == "":   #multiply_divide_list列表第一个字符为空的话，表示一个数字为负号
        multiply_divide_list[1] = "-" + multiply_divide_list[1]
        del plus_minus_operator[0]
        del multiply_divide_list[0]
    res = merge(plus_minus_operator,multiply_divide_list)
    plus_minus_operator=res[0]             #列表 '+' '-' 运算符 进行合并处理
    multiply_divide_list=res[1]
    plus_minus_list=calc_muldiv(multiply_divide_list)       #生成只进行加减运算的列表
    res=calc_plumin(plus_minus_operator,plus_minus_list)
    return  res

def calculate(formula):
    '''计算程序主入口, 主要逻辑是先计算拓号里的值,算出来后再算乘除,再算加减'''
    while True:
        formula_depth = re.search("\([^()]+\)", formula)
        if formula_depth:
            formula_depth=formula_depth.group()
            res=bracket_calc(formula_depth)
            formula=formula.replace(formula_depth,str(res))
            print("\33[34;1m%s\33[0m"%(formula))
        else:
            res = bracket_calc(formula)
            print("\33[31;1m结果:%s\33[0m"%(res))
            exit()

if __name__ == '__main__':
    formula = "1 - 2 * ( (60-30 +(-9-2- 5-2*-3-5/3-40*4/2-3/5+6*3) * (-9-2-5-2*5/3 + 7 /3*-99/4*2998 +10 * 568/14 )) -(-4*3)/ (16-3*2) )"
    calculate(formula)


#1 - 2 * ( (60-30 +-74.26666666666667 * (-9-2-5-2*5/3 + 7 /3*99/4*2998 +10 * 568/14 )) -(-4*3)/ (16-3*2) )
#1 - 2 * ( (60-30 +-74.26666666666667 * 173520.88095238098) -(-4*3)/ (16-3*2) )
#1 - 2 * ( -12886787.425396826 -(-4*3)/ (16-3*2) )
#1 - 2 * ( -12886787.425396826 --12.0/ (16-3*2) )
#1 - 2 * ( -12886787.425396826 --12.0/ 10.0 )
#1 - 2 * -12886786.225396827
#结果:25773573.450793654