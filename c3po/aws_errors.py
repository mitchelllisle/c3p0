class S3Error(Exception):
    """Base class for S3 Errors"""

    class NoParserAvailable(Exception):
        """An error related to no parser being available for the object. E.g. from S3"""

    class NoUploadConversionAvailable(Exception):
        """An error related to uploading data that can't be converted into an appropriate type"""
