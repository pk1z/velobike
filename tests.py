import unittest
from ConfigParser import ConfigParser
from my_parser import DBconnector


class TestDB(unittest.TestCase):
    def setUp(self):
        whole_config = ConfigParser()
        whole_config.read('parser_config.ini')
        config = dict(whole_config.items(whole_config.get('env', 'env')))
        
        self.db = DBconnector(db=config['db'], user=config['db_user'])
        self.tablename = config['tablename']
        
    def tearDown(self):
        self.db.close

    def test_proper_values(self):
        self.db.insert_row(self.tablename, timestamp=100, station_id=999, bikes_cnt=6, free_spases_cnt=4)

    def test_wrong_station(self):
        with self.assertRaises(Exception):
            self.db.insert_row(self.tablename, timestamp=100, station_id='a', bikes_cnt=6, free_spases_cnt=4)


if __name__ == '__main__':
    unittest.main()