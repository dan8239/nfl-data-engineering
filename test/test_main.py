import unittest

from src.main import handler


class TestHandler(unittest.TestCase):
    def test_handler(_):
        val = handler({"message": "!dlroW olleH"}, None)
        assert val == {"message": "!dlroW olleH", "configVar": "Hello World!"}
