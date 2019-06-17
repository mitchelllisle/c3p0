class InputError(Exception):
    """Base class for Input Errors"""

    class NoSecret(Exception):
        """An error related to a missing Secret"""
        pass
