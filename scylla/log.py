import os

import msgpack

from .node import Node


class Log(Node):
    @property
    def path(self):
        """
        Absolute path to the location of the log file on disk.
        """
        return os.path.join(self._location, '{0}.log'.format(self.name))

    def __init__(self, name, location, inbound_port=None):
        super(Log, self).__init__(name, inbound_port=inbound_port)

        self._location = location
        self.add_message_handler(self._handle_message)

    def _handle_message(self, sender, receiver, content):
        content = msgpack.unpackb(content)
        if not content.endswith('\n'):
            content = '{0}\n'.format(content)
        with open(self.path, mode='a') as f:
            f.writelines(content)

    @staticmethod
    def clear(path):
        """
        Clears the log.

        @warning This will delete all data in the log file.
        """
        with open(path, mode='w'):
            pass
