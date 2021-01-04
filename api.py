import http.client
import json
import os

class FootballAPI:
    @staticmethod
    def query(request):
        connection = http.client.HTTPConnection('api.football-data.org')
        headers = { 'X-Auth-Token': os.getenv("API_KEY") }
        connection.request('GET', request, None, headers )
        return json.loads(connection.getresponse().read().decode())


