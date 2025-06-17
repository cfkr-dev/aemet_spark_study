class Validator:
    def __init__(self):
        self.valid = True
        self.error_msgs = []

    def set_invalid(self):
        self.valid = False

    def is_valid(self):
        return self.valid

    def add_error_msg(self, error_msg):
        self.error_msgs.append(error_msg)

    def build_error_message(self):
        message = ""

        for error_msg in self.error_msgs:
            message = message + "validation error: " + error_msg + "\n"

        return message