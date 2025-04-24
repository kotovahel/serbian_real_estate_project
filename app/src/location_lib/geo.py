from fake_headers import Headers
from geopy.geocoders import Nominatim

import time


def get_location(lat, lon):
    geolocator = Nominatim(user_agent=Headers().generate()['User-Agent'], domain='localhost:8080')
    # geolocator = Nominatim(user_agent=Headers().generate()['User-Agent'])
    location = geolocator.reverse(f"{lat}, {lon}", timeout=30)
    print(f'location: {location.address}')
    # return location.address
    return location.address


print(get_location('45.3824', '20.3907'))
