import json
import os
from .inputs_errors import InputError


class Secrets:
    def __init__(self, location=None):
        if location is None:
            location = str(os.getenv("HOME")) + "/.pyprofile"
        text = open(location).read()
        self.config = json.loads(text)

    def load(self, name):
        self.name = name
        if name in self.config.keys():
            self.loaded_credential = self.config.get(name)
        else:
            raise InputError.NoSecret("Secret name {} doesn't exist".format(self.name))

        return self.loaded_credential

    @property
    def names(self):
        return list(self.config.keys())


def source(location=None):
    if location is None:
        location = str(os.getenv("HOME")) + "/.pyprofile"

    text = open(location).read()
    config = json.loads(text)
    return config
