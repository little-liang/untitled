class School(object):
    def __init__(self, name, addr, tel):
        self.school_name = name
        self.addr = addr
        self.tel = tel
        self.stu_list = []
        self.tech_list = []

class School_Member(object):
    def __init__(self, name, age, sex):
        self.name = name
        self.age = age
        self.sex = sex


    def tell(self):
        print('''-----------------
            name:%s
            age:%s
            sex:%s
        ---------------''' % (self.name, self.age, self.sex))

class Student(School_Member):
    def __init__(self, name, age, sex, grade, school):
        # super(Student, self).__init__(name, age, sex)
        School_Member.__init__(self, name, age, sex)
        self.school = school
        self.grade = grade

    #这句是为了让学校知道，自己有多少学生，及相关的学生信息
    #注意，这个列表中的元素全都是实例！是实例
        self.school.stu_list.append(self)

    def tell(self):
        School_Member.tell(self)
        print('''
            school name:%s
            class:%s
            addr:%s
        ''' % (self.school.school_name, self.grade, self.school.addr))

    def pay_money(self):
        print("%s is pay the tution" % (self.name))

    #这是一个从学校的学生列表中删除一个学生的方法，
    def transfer(self):
        #这里并不能把实例从列表中删除，内存地址也没有，必须结合下一条代码
        self.school.stu_list.remove(self)
        #这里是为了把实例（见构造函数）的，传入参数的实例参数，设置为空，这样，self.school这个变量就不能被读到了
        self.school = None

class Teacher(School_Member):
    def __init__(self, name, age, sex, course, salary):
        super(Teacher, self).__init__(name, age, sex)
        # School_Member.__init__(self, name, age, sex)
        self.course = course
        self.salary = salary

    def teaching(self):
        print("%s is teaching the %s" % (self.name, self.course))

#初始化两个学校
school1 = School("putianxueyuan", "putian", 13910733521)
school2 = School("putianxueyuan222", "putian", 114)

#这里所有学生共享一份数据，学校的信息，其实就是学生知道自己学校的信息
s1 = Student("longe", "18", "female", "12", school1)
s11 = Student("longe2", "19", "female", "12", school1)
s12 = Student("longe3", "20", "female", "12", school1)
s13 = Student("longe4", "20", "female", "12", school1)

s1.tell()

#这里代表学校也不是一个，s2是第二个学校的学生
s2 = Student("zhang", "19", "male", "16", school2)


#现在要学校知道自己有那几个学生
for line in school1.stu_list:
    print(line.name, line.school.school_name)

#从学校中删除一个学生
s13.transfer()
print("------------------------------------------")
for line in school1.stu_list:
    print(line.name, line.school.school_name)


# t1 = Teacher("hong", "33", "male", "PY", "80000")

