import random
def random_info(cla,count):
    name = ""
    char = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for i in range(random.randint(2,3)):
        name = name + char[random.randint(0,25)]
    score = random.randint(0, 100)
    # info = str(num) + " " + str(cla) +" "+str(name) +" " + str(score) + "\n"
    info = str(name) + " " + str(cla) + " " + str(score) + "\n"
    return info

#20为生成文件数
for i in range(1,21):
    info = []
    print("class",i)
    file_name = "./class/class" + str(i) + ".txt"
    for j in range(20):
        info.append(random_info(i,j+1))
    print(info)
    with open(file_name,'w+') as f:
        for x in range(len(info)):
            f.writelines(info[x])

