#! python3

#need to use chmod +x test.py to unlock files wtf



def test():
    count = 0
    for i in range(1, 21):
        for j in range(i, 21):
            for k in range(j, 21):
                count += 1
    print(count)

test()
