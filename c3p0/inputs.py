import json
import os

def source(location = None):
    if location == None:
        location = str(os.getenv("HOME")) + "/.pyprofile"
    text = open(location).read()
    config = json.loads(text)
    return config
