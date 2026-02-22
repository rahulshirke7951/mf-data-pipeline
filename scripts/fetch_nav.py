import requests
from datetime import datetime

URL = "https://www.amfiindia.com/spages/NAVAll.txt"

def fetch_nav():
    today = datetime.today().strftime("%Y-%m-%d")
    file_name = f"nav_{today}.txt"

    r = requests.get(URL, timeout=30)

    if r.status_code != 200:
        raise Exception("NAV fetch failed")

    with open(file_name, "w", encoding="utf-8") as f:
        f.write(r.text)

    print("Downloaded:", file_name)

if __name__ == "__main__":
    fetch_nav()
