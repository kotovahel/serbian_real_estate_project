import requests


def get_location(lat, lon, key):
    url = f'https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={key}'
    response = requests.get(url).json()
    print(response['results'][0]['formatted_address'])
    return response['results'][0]['formatted_address']