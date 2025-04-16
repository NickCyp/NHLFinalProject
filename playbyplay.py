import requests
import json
url = 'https://www.nhl.com/penguins/roster'
data=json.load(open('play-by-play.json'))
print(data)

for i in data:
    print(f'{i}:{data[i]}')