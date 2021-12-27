from faker import Faker
from datetime import datetime
fake = Faker(locale='en_US')
t0 = datetime.now()
date = fake.date()
time = fake.time()
with open(r'data.txt','a+')as f:
    for i in range(1000000):
        f.write(fake.date()+' ' + fake.time() +':  seaech-'+ fake.word() + '\n')

print('ok')

t1 = datetime.now()
print(t1-t0)