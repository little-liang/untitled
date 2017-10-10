
import time



def deco(func):
    """
    装饰器，用来检查运行时间。
    """

    def _deco(*args, **kwargs):
        start_time = time.time()
        res = func(*args, **kwargs)
        end_time = time.time()
        print("Total time running %s: %s seconds" %
              (func.__name__, str(end_time - start_time))
              )
        return res

    return _deco


@deco
def getword():
    for line in range(1, 10):
        print(line)
        time.sleep(1)

getword()
