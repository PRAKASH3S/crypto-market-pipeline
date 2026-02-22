import requests
from datetime import datetime
COINS = ["bitcoin", "ethereum", "solana", "cardano"]

def fetch_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params ={
        "ids": ",".join(COINS),
        "vs_currencies": "usd",
        "include_24hr_change": "true"
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    print(f"Fetched at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 40)

    for coin, values in data.items():
        price = values["usd"]
        change = round(values["usd_24h_change"], 2)
        direction = "UP" if change > 0 else "DOWN"
        print(f"{direction} {coin.upper():<12} ${price:>10,.2f} ({change}%)")
if __name__ == "__main__":
    fetch_prices()
    
        

