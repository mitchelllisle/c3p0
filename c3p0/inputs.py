import json

def pyprofile(file = "~/pyprofile.json"):
    text =  open(file).read()
    profile = json.loads(text)
    return profile
