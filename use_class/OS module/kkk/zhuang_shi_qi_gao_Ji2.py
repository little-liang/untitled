from functools import wraps
def deco(argv):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            print("wrapper")
            return func(*args, **kwargs)

        print("decorator")
        return wrapper

    print("deco")
    print(argv)
    return decorator


@deco("123")
def foo(data):
    "this is foo"
    print("foo")
    print(data)

foo("afdasdf")

print(foo.__name__, foo.__doc__)