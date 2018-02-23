string = 'Hello,how,are,you,"This is a number 100,000,000",hello,how,are,you,"Another Number 1000,0,0,0,",keep,going'

print(string)
number = 0
count = 0
for x, count in string:
    if x == "\"":
        number = number + 1
    if x == ',' and number % 2 != 0:
        string = string[:count] + "|" + string[count:]
    count = count + 1
    print(number)

print(string)