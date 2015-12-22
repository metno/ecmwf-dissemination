class ECReceiveException(Exception):
    pass


class ECReceiveProductstatusException(ECReceiveException):
    """
    Thrown when there is insufficient data available at the Productstatus server.
    """
    pass


class InvalidDataException(ECReceiveException):
    pass


class InvalidFilenameException(ECReceiveException):
    """
    Thrown when a filename does not fit into the expected format.
    """
    pass


class TryAgainException(ECReceiveException):
    """
    Thrown when file processing fails because of some external component
    failure, but the application should retry the processing.
    """
    pass
