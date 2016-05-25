import MySQLdb
import pdb
import os
import glob
import json
import pdb


def dict_to_tuples(d):
    """
    returns two tuples of the dict keys and values correspondingly, the values order matches the keys
    """
    keys = tuple(d.keys())
    values = tuple([str(d[k]) for k in keys])
    return keys, values


class DBconnector(object):
    
    def __init__(self, db, **kwargs):
        """
        :param db: database to use.
        :param host: name of host to connect to. Default: use the local host via a UNIX socket (where applicable)
        :param user: user to authenticate as. Default: current effective user.
        :param passwd: password to authenticate with. Default: no password.
        :param port: TCP port of MySQL server. Default: standard port (3306).
        other params: see http://mysql-python.sourceforge.net/MySQLdb.html#functions-and-attributes
        """
        self.db = MySQLdb.connect(db=db, **kwargs)
        self.cursor = self.db.cursor()
        
    def insert_row(self, tablename, **kwargs):
        keys, values = dict_to_tuples(kwargs)
        
        # pdb.set_trace()
        query = "INSERT INTO %s" % (tablename,) + \
              "(" + ", ".join(["%s"]*len(kwargs)) % keys + \
              ") VALUES (" + ",".join(["%s"]*len(kwargs)) + ")" 
              
        
        self.cursor.execute(query, values)
        self.db.commit()
    
    def insert_factory(self, tablename, keys):
        query = "INSERT INTO %s" % (tablename,) + \
              "(" + ", ".join(["%s"]*len(keys)) % keys + \
              ") VALUES (" + ",".join(["%s"]*len(keys)) + ")"
        def insert(list_of_rows):
            self.cursor.executemany(query, list_of_rows)
            self.db.commit()
        return insert
    
    def insert_rows(self, tablename, keys, values):
        """
        :param keys: tuple of column names
        :param values: list of tuples of values
        """
        query = "INSERT INTO %s" % (tablename,) + \
              "(" + ", ".join(["%s"]*len(keys)) % keys + \
              ") VALUES (" + ",".join(["%s"]*len(keys)) + ")"
        self.cursor.executemany(query, values)  
        self.db.commit()
        
    def check_presense(self, tablename, key, value):    
        query = "SELECT * from %s" % (tablename,) + \
                " WHERE %s=%%s" % (key)
        self.cursor.execute(query, (value,))
        return self.cursor.fetchone() is not None
    
    
    def close(self):
        self.cursor.close()
        self.db.close()
    
    
def extract_velobike_json(filename):
    with open(filename) as f:
        data = json.load(f)
    return data['Items']
    
        
def extract_data(folder, file_mask,
                 sorting_key=os.path.getctime,
                 extractor=extract_velobike_json):
    filename = max(glob.iglob(os.path.join(folder, file_mask)), key=sorting_key)
    timestamp = filename.split('/')[-1].split('_')[1]
    return timestamp, extractor(filename)
    

def process_folder(foldername, file_mask, timestamp, is_obsolete, extract, prepare, insert):
    filenames = sorted(glob.iglob(foldername + '/' + file_mask), key=lambda x: -timestamp(x))
    for filename in filenames:
        print 'processing ' + filename
        if is_obsolete(filename):
            print 'file ' + filename + ' already processed'
        else:
            data_list = extract(filename)
            data = prepare(data_list, timestamp(filename))
            insert(data)


def prepare_data_list(data_list, directions_list):
    return [tuple(direction(entry) for direction in directions_list) for entry in data_list]


if __name__ == '__main__':
    from ConfigParser import ConfigParser
    whole_config = ConfigParser()
    whole_config.read('parser_config.ini')
    config = dict(whole_config.items(whole_config.get('env', 'env')))
    
    parse_timestamp = lambda filename: int(filename.split('/')[-1].split('_')[1])
    
    db = DBconnector(host=config['db_host'],
                     port=int(config['db_port']),
                     db=config['db'],
                     user=config['db_user'],
                     passwd=config['db_pwd'])
    
    COLUMNS = ('timestamp', 'station_id', 'bikes_cnt', 'free_spases_cnt')
    
    insert = db.insert_factory(config['tablename'], COLUMNS)
    prepare = lambda data_list, timestamp: prepare_data_list(data_list,
                                                             [lambda d: timestamp,
                                                              lambda d: d['Id'],
                                                              lambda d: d['TotalPlaces']-d['FreePlaces'],
                                                              lambda d: d['FreePlaces']])
    
    process_folder(foldername=config['folder'],
                   file_mask=config['file_mask'], 
                   timestamp=parse_timestamp,
                   is_obsolete=lambda filename: db.check_presense(config['tablename'], 'timestamp', parse_timestamp(filename)),
                   extract=extract_velobike_json,
                   prepare=prepare,
                   insert=insert)
    
    
    db.close()
