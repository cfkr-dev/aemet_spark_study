from App.Utils.Storage.Core.storage import Storage


class Validator:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.valid = True
        self.error_msgs = []
        self.custom_data = {}

    def set_invalid(self):
        self.valid = False

    def is_valid(self):
        return self.valid

    def add_error_msg(self, error_msg):
        self.error_msgs.append(error_msg)

    def add_custom_data(self, key, value):
        self.custom_data[key] = value

    def get_custom_data(self, key):
        return self.custom_data[key]

    def build_error_message(self):
        message = ""

        for error_msg in self.error_msgs:
            message = message + "validation error: " + error_msg + "\n"

        return message