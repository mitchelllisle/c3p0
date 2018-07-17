import json
import os

class profile:
    '''
    env = profile()
    env.read()
    env.add({"hello" : {"test" : "tetOne"}})
    env.delete("hello")
    env.profile
    
    profile is a way to manage keys, secrets and other information that you want
    to store for your Python environment. Reason for this came from no real alternative
    that works in a similar way to the way R works with its .Rprofile.

    Basics of this class are:
    create: Add a new profile. All profiles are json documents
    add: Add a key value pair, or nested object to your profile
    delete: Remove key from profile
    read: Read profile

    If you have a profile in your HOME directory it will default to using that. Note: Mac is the only OS
    supported for this.
    '''
    def __init__(self):
        self.profile = {}

    def create(self, config = {}, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        assert type(config) == dict
        if os.path.isfile(location):
            raise Exception(location + " is already a valid profile. Please choose another location or use `add` to add to existing profile.")

        self.profile.update(config)
        with open(location, 'w') as outfile:
            json.dump(self.profile, outfile)
        return location

    def read(self, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        text = open(location).read()
        config = json.loads(text)
        self.profile.update(config)

    def add(self, config, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        text = open(location).read()
        oldconfig = json.loads(text)
        self.profile.update(oldconfig)
        self.profile.update(config)
        with open(location, 'w') as outfile:
            json.dump(self.profile, outfile)

    def delete(self, config, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        text = open(location).read()
        oldconfig = json.loads(text)
        self.profile.update(oldconfig)
        self.profile.pop(config)
        with open(location, 'w') as outfile:
            json.dump(self.profile, outfile)
