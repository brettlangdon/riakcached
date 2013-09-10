class InlineClass(object):
    """
    Thanks Smashery: http://stackoverflow.com/a/1528939/2040727
    """
    def __init__(self, dict):
        self.__dict__ = dict
