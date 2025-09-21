import requests

pairs = ["EURUSDT","GBPUSDT","AUDUSDT","NZDUSDT","USDJPY","USDCAD","USDCHF","EURJPY","GBPJPY","AUDJPY"]

for s in pairs:
    r = requests.get(f"https://api.binance.com/api/v3/exchangeInfo?symbol={s}")
    if r.status_code == 200:
        data = r.json()
        if data.get("symbols"):
            print(s, "✅ OK")
        else:
            print(s, "❌ NOT FOUND (empty symbols)")
    else:
        print(s, "❌ Error", r.status_code, r.text)
