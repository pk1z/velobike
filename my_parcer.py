import MySQLdb
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
        
    def close(self):
        self.cursor.close()
        self.db.close()

if __name__ == '__main__':
    from ConfigParser import ConfigParser
    whole_config = ConfigParser()
    whole_config.read('parser_config.ini')
    config = dict(whole_config.items(whole_config.get('env', 'env')))
    
    db = DBconnector(db=config['db'], user=config['db_user'])
    
    db.insert_row(config['tablename'], timestamp=102, station_id='a', bikes_cnt=6, free_spases_cnt=4)
    db.close()
    