import os

from .core import Node


class Log(Node):
    @property
    def path(self):
        """
        Absolute path to the location of the log file on disk.
        """
        return os.path.join(self._location, '{0}.log'.format(self.name))

    def __init__(self, name, location, message_port=None):
        super(Log, self).__init__(name, message_port=message_port)

        self._location = location
        self.add_message_handler(self._handle_message)

    def _handle_message(self, msg_type, msg_source, msg_body, msg_receiver):
        if not msg_body.endswith('\n'):
            msg_body = '{0}\n'.format(msg_body)
        with open(self.path, mode='a') as f:
            f.writelines(msg_body)

    def _handle_clear(self, msg_type, msg_source, msg_body, msg_receiver):
        Log.clear(self.path)

    @staticmethod
    def clear(path):
        """
        Clears the log.

        @warning This will delete all data in the log file.
        """
        with open(path, mode='w'):
            pass
