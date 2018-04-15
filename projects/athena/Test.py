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

with gzip.open(bytestream, 'rt') as file:
    print('Creating new zip')
    file_buffer = BytesIO()
    with gzip.GzipFile(fileobj=file_buffer, mode='w') as f:
        for row in file.readlines():
            if 'identity/LineItemId' in row:
                originalList = row.rstrip().split(',')
                uniqueList = []

                for index, item in enumerate(originalList):
                    if item.lower() in uniqueList:
                        originalList[index] = item + str(uniqueList.count(item.lower()))
                        uniqueList.append(item.lower())
                    else:
                        uniqueList.append(item.lower())

                row = ','.join(originalList) + '\n'
            else:
                #print(row)
                #year = re.search(".+?(\d+)-(\d+)-(\d+).*", row).group(1)
                #month = re.search(".+?(\d+)-(\d+)-(\d+).*", row).group(2)
                #day = re.search(".+?(\d+)-(\d+)-(\d+).*", row).group(3)

                #print(year)
                #print(month)
                #print(day)
                # Split on quotes

                lineAsList = row.split('"')
                # If index odd, the item is between quotes, so replace all commas with escaped commas
                for i, part in enumerate(lineAsList):
                    # Replace on odds only
                    if i % 2 != 0:
                        lineAsList[i] = part.replace(",", ".")
                # Rejoin line as string
                row = ''.join(lineAsList)
                #print(row)
                #time.sleep(1)
            f.write(row.encode('utf-8'))