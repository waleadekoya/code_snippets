from config import CONNECTION_STRING
print(CONNECTION_STRING.split('?')[0])

print(CONNECTION_STRING.split('/')[:-1])