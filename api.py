import http.client
import json
import os
import requests
import bs4
from scrapy import Request


class FootballAPI:
    @staticmethod
    def query(request):
        connection = http.client.HTTPConnection("api.football-data.org")
        headers = {"X-Auth-Token": os.getenv("API_KEY")}
        connection.request("GET", request, None, headers)
        return json.loads(connection.getresponse().read().decode())


def get_lineup_for_match(match_id: int):
    url = f"https://www.football-lineups.com/match/{match_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36"
    }
    html = requests.get(url, headers=headers)
    page = bs4.BeautifulSoup(html.text)
    team_table = page.find("table", id="mainarea")


def get_lineup_two(match_id: int):
    url = f"https://www.football-lineups.com/match/{match_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36",
        "Referer": url,
        "Host": "www.football-lineups.com",
        "Origin": "https://www.football-lineups.com",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "_ga=GA1.2.1903685950.1619283786; __qca=P0-610996095-1619283787231; PHPSESSID=1vq1ulns8puu3l01i9i9it9ir5; euconsent-v2=CPFKxrXPFKxrXAKAcAENBXCsAP_AAH_AAACIHvtf_X__b39j-_59__t0eY1f9_7_v-0zjhfdt-8N2f_X_L8X42M7vF36pq4KuR4Eu3LBIQNlHOHUTUmw6okVrTPsak2Mr7NKJ7LEinMbe2dYGHtfn91TuZKYr_78_9fz__-__v___9f3r-3_3__59X---_e_V399zLv9__34HugEmGpfABZiWODJNGlUKIEIVhIdAKACihGFomsIGVwU7K4CPUEDABCagIwIgQYgoxYBAAIBAEhEQEgB4IBEARAIAAQAqQEIACJgEFgBYGAQACgGhYgRQBCBIQZHBUcpgQESLRQT2VgCUXexphCGUWAFAo_oqMBEoQQLAyEgAAAA.f_gAAAAAAAAA; addtl_consent=1~39.4.3.9.6.5.4.13.6.4.15.9.5.2.7.4.1.7.1.3.2.10.3.5.4.21.4.6.9.7.10.2.9.2.12.6.7.6.14.5.20.6.5.1.3.1.11.29.4.14.4.5.3.10.6.2.9.6.6.4.5.3.1.4.29.4.5.3.1.6.2.2.17.1.17.10.9.1.8.6.2.8.3.4.142.4.8.35.7.15.1.14.3.1.8.10.14.11.3.7.25.5.18.9.7.41.2.4.18.21.3.4.2.1.6.6.5.2.14.18.7.3.2.2.8.20.8.8.6.3.10.4.20.2.4.9.3.1.6.4.11.1.3.22.16.2.6.8.2.4.11.6.5.17.16.11.8.1.10.28.8.4.1.3.21.2.7.6.1.9.30.17.4.9.15.8.7.3.6.6.7.2.4.1.7.12.13.22.13.2.12.2.4.6.1.4.15.2.4.9.4.5.1.3.7.13.5.15.4.13.4.14.8.2.15.2.5.5.1.2.2.1.2.14.7.4.8.2.9.10.18.12.13.2.18.1.1.3.1.1.9.25.4.20.8.4.5.2.1.5.4.8.4.2.2.2.14.2.13.4.2.6.9.6.3.4.3.5.2.3.6.10.11.2.4.3.16.3.11.3.1.2.3.9.19.11.15.3.10.7.6.4.3.4.9.3.3.3.1.1.1.6.11.3.1.1.7.4.6.1.10.5.2.6.3.2.2.4.3.2.2.7.2.13.7.12.2.1.6.4.5.4.3.2.2.4.1.3.1.1.1.2.9.1.6.9.1.5.2.1.7.2.8.11.1.3.1.1.2.1.3.2.6.1.5.6.1.5.3.1.3.1.1.2.2.7.7.1.4.1.2.6.1.2.1.1.3.1.1.4.1.1.2.1.8.1.7.4.3.2.1.3.1.4.3.9.6.1.15.10.28.1.2.1.1.12.3.4.1.6.3.4.7.1.3.1.1.3.1.5.3.1.3.2.2.1.1.4.2.1.2.1.1.1.2.2.4.2.1.2.2.2.4.1.1.1.2.1.1.1.1.1.1.1.1.1.1.1.2.2.1.1.2.1.2.1.7.1.2.1.1.1.2.1.1.1.1.2.1.1.3.2.1.1.8.1.1.1.5.2.1.6.5.1.1.1.1.1.2.2.3.1.1.4.1.1.2.2.1.1.4.2.1.1.2.3.2.1.2.3.1.1.1.1.4.1.1.1.5.1.9.3.1.5.1.1.3.4.1.2.3.1.4.2.1.2.2.2.1.1.1.1.1.1.11.1.3.1.1.2.2.1.4.2.3.2.1.4.1.1.1.1.1.3.2.1.1.2.5.1.3.6.4.1.1.3.1.4.3.1.4.5.1.7.2.1.1.1.2.1.1.1.4.2.1.12.1.1.3.1.2.2.3.1.3.1.1.2.1.1.2.1.1.1.1.2.4; trc_cookie_storage=taboola%20global%3Auser-id=9b740746-79b0-4e03-9a53-2649d5ba84c7-tuct45e6785; mixpanelId=17904d6c16a4f1-0cd68c45860f35-33687008-13c680-17904d6c16b54b; _hjTLDTest=1; _hjid=72c5cad6-203e-43df-af4b-7c4c1d7d3448; _gid=GA1.2.856584023.1619469092; panoramaId_expiry=1620073902272; panoramaId=d63dab89bee9e4281f72bc348fe016d539388b5a8db644abbade7382ceff76bf; flmail=jimjam124@mailinator.com; flpass=5104461a26b8ff2d4f5767bcc53f9916; _gat=1; _gat_UA-127320976-2=1; lotame_domain_check=football-lineups.com",
    }
    data = {"f": match_id, "w": 670, "h": 194, "m": 8, "i": 0, "mn": 0}
    html = requests.post("https://www.football-lineups.com/ajax/get_lnp.php", data=data)
    page = bs4.BeautifulSoup(html.text)
    return page


def scrape_lineup(match_id):
    url = f"https://www.football-lineups.com/match/{match_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36",
        "Referer": url,
        "Host": "www.football-lineups.com",
        "Origin": "https://www.football-lineups.com",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "_ga=GA1.2.1903685950.1619283786; __qca=P0-610996095-1619283787231; PHPSESSID=1vq1ulns8puu3l01i9i9it9ir5; euconsent-v2=CPFKxrXPFKxrXAKAcAENBXCsAP_AAH_AAACIHvtf_X__b39j-_59__t0eY1f9_7_v-0zjhfdt-8N2f_X_L8X42M7vF36pq4KuR4Eu3LBIQNlHOHUTUmw6okVrTPsak2Mr7NKJ7LEinMbe2dYGHtfn91TuZKYr_78_9fz__-__v___9f3r-3_3__59X---_e_V399zLv9__34HugEmGpfABZiWODJNGlUKIEIVhIdAKACihGFomsIGVwU7K4CPUEDABCagIwIgQYgoxYBAAIBAEhEQEgB4IBEARAIAAQAqQEIACJgEFgBYGAQACgGhYgRQBCBIQZHBUcpgQESLRQT2VgCUXexphCGUWAFAo_oqMBEoQQLAyEgAAAA.f_gAAAAAAAAA; addtl_consent=1~39.4.3.9.6.5.4.13.6.4.15.9.5.2.7.4.1.7.1.3.2.10.3.5.4.21.4.6.9.7.10.2.9.2.12.6.7.6.14.5.20.6.5.1.3.1.11.29.4.14.4.5.3.10.6.2.9.6.6.4.5.3.1.4.29.4.5.3.1.6.2.2.17.1.17.10.9.1.8.6.2.8.3.4.142.4.8.35.7.15.1.14.3.1.8.10.14.11.3.7.25.5.18.9.7.41.2.4.18.21.3.4.2.1.6.6.5.2.14.18.7.3.2.2.8.20.8.8.6.3.10.4.20.2.4.9.3.1.6.4.11.1.3.22.16.2.6.8.2.4.11.6.5.17.16.11.8.1.10.28.8.4.1.3.21.2.7.6.1.9.30.17.4.9.15.8.7.3.6.6.7.2.4.1.7.12.13.22.13.2.12.2.4.6.1.4.15.2.4.9.4.5.1.3.7.13.5.15.4.13.4.14.8.2.15.2.5.5.1.2.2.1.2.14.7.4.8.2.9.10.18.12.13.2.18.1.1.3.1.1.9.25.4.20.8.4.5.2.1.5.4.8.4.2.2.2.14.2.13.4.2.6.9.6.3.4.3.5.2.3.6.10.11.2.4.3.16.3.11.3.1.2.3.9.19.11.15.3.10.7.6.4.3.4.9.3.3.3.1.1.1.6.11.3.1.1.7.4.6.1.10.5.2.6.3.2.2.4.3.2.2.7.2.13.7.12.2.1.6.4.5.4.3.2.2.4.1.3.1.1.1.2.9.1.6.9.1.5.2.1.7.2.8.11.1.3.1.1.2.1.3.2.6.1.5.6.1.5.3.1.3.1.1.2.2.7.7.1.4.1.2.6.1.2.1.1.3.1.1.4.1.1.2.1.8.1.7.4.3.2.1.3.1.4.3.9.6.1.15.10.28.1.2.1.1.12.3.4.1.6.3.4.7.1.3.1.1.3.1.5.3.1.3.2.2.1.1.4.2.1.2.1.1.1.2.2.4.2.1.2.2.2.4.1.1.1.2.1.1.1.1.1.1.1.1.1.1.1.2.2.1.1.2.1.2.1.7.1.2.1.1.1.2.1.1.1.1.2.1.1.3.2.1.1.8.1.1.1.5.2.1.6.5.1.1.1.1.1.2.2.3.1.1.4.1.1.2.2.1.1.4.2.1.1.2.3.2.1.2.3.1.1.1.1.4.1.1.1.5.1.9.3.1.5.1.1.3.4.1.2.3.1.4.2.1.2.2.2.1.1.1.1.1.1.11.1.3.1.1.2.2.1.4.2.3.2.1.4.1.1.1.1.1.3.2.1.1.2.5.1.3.6.4.1.1.3.1.4.3.1.4.5.1.7.2.1.1.1.2.1.1.1.4.2.1.12.1.1.3.1.2.2.3.1.3.1.1.2.1.1.2.1.1.1.1.2.4; trc_cookie_storage=taboola%20global%3Auser-id=9b740746-79b0-4e03-9a53-2649d5ba84c7-tuct45e6785; mixpanelId=17904d6c16a4f1-0cd68c45860f35-33687008-13c680-17904d6c16b54b; _hjTLDTest=1; _hjid=72c5cad6-203e-43df-af4b-7c4c1d7d3448; _gid=GA1.2.856584023.1619469092; panoramaId_expiry=1620073902272; panoramaId=d63dab89bee9e4281f72bc348fe016d539388b5a8db644abbade7382ceff76bf; flmail=jimjam124@mailinator.com; flpass=5104461a26b8ff2d4f5767bcc53f9916; _gat=1; _gat_UA-127320976-2=1; lotame_domain_check=football-lineups.com",
    }
    data = {"f": match_id, "w": 670, "h": 194, "m": 8, "i": 0, "mn": 0}
    html = requests.post("https://www.football-lineups.com/ajax/get_lnp.php", data=data)
    req = Request(url, headers=headers)
    return req


if __name__ == "__main__":
    r = scrape_lineup(350049)
