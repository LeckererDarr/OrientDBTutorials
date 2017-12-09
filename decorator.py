# decorator
def exception(func):
    def deco(*args, **kwargs):
        print(1)
        func()
        print(2)
        return func()
    return deco()

@exception
def m():
    print(99)

a = m()