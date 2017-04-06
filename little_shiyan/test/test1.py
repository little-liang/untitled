import time,sys

def foo(func):
    def _deco(a, b):
        print(a, b)
        ret = func(a, b)
        return ret
    return _deco


@foo
def movie(kk, hh):
    for line in range(0, 3):
        content = "%s ...ggg " % (line)
        print(kk, hh, content)
        sys.stdout.flush()
        return "ninhao"


movie(1,2)

