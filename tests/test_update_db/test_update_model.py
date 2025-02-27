import unittest
import sys
import os
update_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/'))
sys.path.append(update_db_path)
import update_db.update_model as update_model


class TestUpdateModel(unittest.TestCase):
    def test_load_model(self):
        pass


if __name__ == '__main__':
    unittest.main()
