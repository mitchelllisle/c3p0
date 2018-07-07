import json
import os

class pyprofile:
    '''
    Pyprofile is a way to manage keys, secrets and other information that you want
    to store for your Python environment. Reason for this came from no real alternative
    that works in a similar way to .Rprofile.
    Basics of this class are:
    create: Add a new profile. All profiles are json documents
    add: Add a key value pair, or nested object to your profile
    delete: Remove key from profile
    read: Read profile
    '''
    def create(config = {}, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        assert type(config) == dict
        if os.path.isfile(location):
            raise Exception(location + " is already a valid profile. Please choose another location or use `add` to add to existing profile.")

        pyprofile = {}
        pyprofile.update(config)
        with open(location, 'w') as outfile:
            json.dump(pyprofile, outfile)
        return location

    def read(location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        text =  open(location).read()
        profile = json.loads(text)
        return profile

    def add(config, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        profile = pyprofile.read(location)
        profile.update(config)
        with open(location, 'w') as outfile:
            json.dump(profile, outfile)
        return profile

    def delete(config, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        profile = pyprofile.read(location)
        profile.pop(config)
        with open(location, 'w') as outfile:
            json.dump(profile, outfile)
        return profile
