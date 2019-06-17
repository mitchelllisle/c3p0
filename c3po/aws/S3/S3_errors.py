class S3Error(Exception):
    """Base class for S3 Errors"""

    class NoParserAvailable(Exception):
        """An error related to no parser being available for the object. E.g. from S3"""
