# -*- coding: utf-8 -*-

classmates = ['Michael', 'Bob', 'Tracy']
print(classmates)
print(len(classmates))
print(classmates[1])
print(classmates[-1])
classmates.append('Adam')
print(classmates)
classmates.insert(1, 'Jack')
print(classmates)
classmates.pop()
print(classmates)
classmates.pop(0)
print(classmates)
classmates[1]='Sarah'
print(classmates)

L = ['Apple', 123, True]
print(L)

s = ['python','java',['asp','php'],'sheme']
print(s)

f = [False, True]

s.append(f)
print(s)
print(s[4][1])