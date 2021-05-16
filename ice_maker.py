import os
import mysql.connector


'''
CREATE TABLE icecubes (sha512 VARCHAR(128) PRIMARY KEY, fn VARCHAR(1024), 
dir VARCHAR(1024), sha1 VARCHAR(40), sha256 VARCHAR(64), md5 VARCHAR(32), 
ed2k VARCHAR(32), xxh128 VARCHAR(32), metadata JSON, size BIGINT);
'''

class IceMaker:
    def __init__(self, table=None):
        self.DB_USER = os.environ.get('MYSQL_USER')
        self.DB_HOST = os.environ.get("MYSQL_HOST")
        self.DB_PW = os.environ.get('MYSQL_PASSWORD')
        self.DB_NAME = os.environ.get('MYSQL_DATABASE')
        self.DB_TABLE = table
        self.error_table = 'errors'
        self.hash_table = 'hash_fail'

    def __enter__(self):
        self.__conn = mysql.connector.connect(
            host=self.DB_HOST,
            user=self.DB_USER,
            database=self.DB_NAME,
            password=self.DB_PW
        )
        self.__cursor = self.__conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__conn.commit()
        self.__cursor.close()
        self.__conn.close()

    def freeze(self, **kwargs):
        query = f'INSERT into {self.DB_TABLE}'\
                ' (sha512, fn, dir, sha1, sha256, md5, ed2k, xxh128, metadata, fsize)'\
                'VALUES (%(sha512)s, %(fn)s, %(dir)s, %(sha1)s, %(sha256)s,'\
                ' %(md5)s, %(ed2k)s, %(xxh128)s, %(metadata)s, %(fsize)s)'
        self.__cursor.execute(query, kwargs)

    def error(self, file_path):
        file_dir = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)
        query = f'INSERT into errors (fn, dir) VALUES (%(fn)s, %(dir)s)'
        self.__cursor.execute(query, {'fn': file_name, 'dir': file_dir})

    def hash_fail(self, file_path):
        file_dir = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)
        query = f'INSERT into hash_fail (fn, dir) VALUES (%(fn)s, %(dir)s)'
        self.__cursor.execute(query, {'fn': file_name, 'dir': file_dir})

    def exists_hash(self, hash_id):
        query = f'SELECT count(sha512) from {self.DB_TABLE} where sha512 like %(sha512)s'
        self.__cursor.execute(query, {'sha512': hash_id})
        match_count_tup = self.__cursor.fetchone()
        if match_count_tup[0] == 1:
            return True
        else:
            return False

    def exists(self, file_path):
        query = f'SELECT count(sha512) from {self.DB_TABLE} where fn LIKE %(fn)s AND dir LIKE 5(dir)s'
        self.__cursor.execute(query, {'dir': os.path.dirname(file_path), 'fn': os.path.basename(file_path)})
        match_count_tup = self.__cursor.fetchone()
        if match_count_tup[0] == 1:
            return True
        else:
            return False

    def drop_table(self, table):
        query = f'DROP table %(table_name)s'
        self.__cursor.execute(query, {'table_name': table})
