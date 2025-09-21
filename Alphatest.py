import requests, json

API_KEY = "K73NI91K1QPCWSHM"
url = f"https://www.alphavantage.co/query?function=FX_INTRADAY&from_symbol=EUR&to_symbol=USD&interval=1min&apikey={API_KEY}&outputsize=compact"
r = requests.get(url)
print(r.status_code)
print(json.dumps(r.json(), indent=2))

