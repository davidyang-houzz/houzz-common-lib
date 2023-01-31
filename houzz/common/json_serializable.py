__author__ = 'jasonl'


class JSONSerializable(object):
    def to_json(self):
        return self.__dict__
