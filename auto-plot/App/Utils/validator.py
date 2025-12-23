"""Simple validation helper used by model classes.

The :class:`Validator` object holds a storage backend reference and a
list of validation messages that are populated when model configuration
checks fail. It is intentionally lightweight and used across many model
validators.
"""

from App.Utils.Storage.Core.storage import Storage


class Validator:
    """Collector for configuration validation results.

    :ivar storage: Storage backend instance used by validators that need I/O checks.
    :type storage: App.Utils.Storage.Core.storage.Storage
    :ivar valid: Boolean flag that indicates overall validity (defaults to True).
    :type valid: bool
    :ivar error_msgs: List of human-readable error messages accumulated.
    :type error_msgs: list[str]
    :ivar custom_data: Arbitrary dictionary used to pass contextual values between validators.
    :type custom_data: dict
    """

    def __init__(self, storage: Storage):
        """Create a new :class:`Validator`.

        :param storage: Storage backend used by validators for path existence checks.
        :type storage: App.Utils.Storage.Core.storage.Storage
        """
        self.storage = storage
        self.valid = True
        self.error_msgs = []
        self.custom_data = {}

    def set_invalid(self):
        """Mark the current validation as failed (``valid`` becomes False)."""
        self.valid = False

    def is_valid(self):
        """Return the current validation boolean state.

        :returns: True when no validation errors were added, False otherwise.
        :rtype: bool
        """
        return self.valid

    def add_error_msg(self, error_msg):
        """Append a human-readable error message to the collector.

        :param error_msg: Error message to append.
        :type error_msg: str
        """
        self.error_msgs.append(error_msg)

    def add_custom_data(self, key, value):
        """Store arbitrary contextual data accessible to other validators.

        :param key: Dictionary key.
        :param value: Associated value to store.
        """
        self.custom_data[key] = value

    def get_custom_data(self, key):
        """Retrieve a previously stored custom data value.

        :param key: Key to retrieve from the custom data mapping.
        :returns: The stored value for ``key``.
        """
        return self.custom_data[key]

    def build_error_message(self):
        """Build and return a single textual message containing all errors.

        :returns: A multi-line string containing all accumulated validation messages.
        :rtype: str
        """
        message = ""

        for error_msg in self.error_msgs:
            message = message + "validation error: " + error_msg + "\n"

        return message