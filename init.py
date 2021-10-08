import mysql.connector
import redis
import const

def create_table():
    conn = mysql.connector.connect(user='wangzz15', password='123456', database='olympic')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS `entrance`(
        `id` INT UNSIGNED AUTO_INCREMENT,
        `person_id` VARCHAR(15) NOT NULL,
        `time` DATETIME NOT NULL,
        `station_id` VARCHAR(15) NOT NULL,
        PRIMARY KEY ( `id` )
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS `exit`(
        `id` INT UNSIGNED AUTO_INCREMENT,
        `person_id` VARCHAR(15) NOT NULL,
        `time` DATETIME NOT NULL,
        `station_id` VARCHAR(15) NOT NULL,
        PRIMARY KEY ( `id` )
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
    ''')
    conn.commit()
    cursor.close()

def redis_init():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    # todo
    r.sadd(const.STATION_SET_KEY, "345", "456")
    r.close()

if __name__ == "__main__":
    create_table()
    redis_init()

