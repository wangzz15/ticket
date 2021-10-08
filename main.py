from flask import Flask
from flask import request
import mysql.connector
import redis
import json
from datetime import datetime
import const

app = Flask(__name__)



def gen_key(head, id):
    return head + id

@app.route("/entrance")
def entrance():    
    return _insert("entrance")

@app.route("/exit")
def exit():
    return _insert("exit")

def _insert(table):
    conn = mysql.connector.connect(user='wangzz15', password='123456', database='olympic')
    cursor = conn.cursor()
    person_id = request.args.get('person_id', '')
    time = request.args.get('time', '')
    station_id = request.args.get('station_id', '')
    if not check(person_id, time, station_id): 
        return "err"
    cursor.execute('insert into ' + table + ' (person_id, time, station_id) values (%s, %s, %s)', [person_id, time, station_id])
    conn.commit()
    cursor.close()
    conn.close()
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    last_arrive = bytes.decode(r.hget(const.PERSON_LAST_ARRIVE_HASH_KEY, person_id))
    # todo: not last_arrive? or last_arrive == ''?
    if last_arrive:
        r.hincrby(gen_key(const.STREAM_FROM_HEAD, last_arrive), person_id, amount=1)
    r.hset(const.PERSON_LAST_ARRIVE_HASH_KEY, person_id, station_id)
    key = gen_key(const.STATION_COUNT_HEAD, station_id)
    if table == "entrance":
        r.incr(key, amount=1)
    else:
        r.decr(key, amount=0)
    r.close()
    return "ok"

# todo 
def check(person_id, time, station_id):
    if person_id == "" or time == "" or station_id == "":
        return False
    return True



@app.route("/query_for_track")
def query_for_track():
    conn = mysql.connector.connect(user='wangzz15', password='123456', database='olympic')
    person_id = request.args.get('person_id', '')

    cursor = conn.cursor()
    cursor.execute('select person_id, time, station_id from entrance where person_id = %s order by time', [person_id])
    entrance_res = cursor.fetchall()
    for i in range(len(entrance_res)):
        entrance_res[i] = {'person_id': entrance_res[i][0], 'time': entrance_res[i][1].strftime(const.TIME_STYLE), 'station_id': entrance_res[i][2]}
    cursor.close()

    cursor = conn.cursor()
    cursor.execute('select person_id, time, station_id from exit where person_id = %s order by time', [person_id])
    exit_res = cursor.fetchall()
    for i in range(len(entrance_res)):
        exit_res[i] = {'person_id': exit_res[i][0], 'time': exit_res[i][1].strftime(const.TIME_STYLE), 'station_id': exit_res[i][2]}
    conn.close()
    return json.dumps({'entrance': entrance_res, 'exit': exit_res})

@app.route("/query_for_density")
def query_for_density():
    station_id = request.args.get('station_id', '')
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    if station_id == '':
        res = {}
        s = r.smembers(const.STATION_SET_KEY)
        for c in s:
            station_id = bytes.decode(c)
            key = gen_key(const.STATION_COUNT_HEAD, station_id)
            res[station_id] = r.get(key)
        return json.dumps({'density': res})
    else:
        key = gen_key(const.STATION_COUNT_HEAD, station_id)
        return json.dumps({'density': {station_id: r.get(key)}})

@app.route("/query_for_stream")
def query_for_stream():
    from_station_id = request.args.get('from_station_id', '')
    to_station_id = request.args.get('to_station_id', '')
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    if from_station_id == '':
        res = {}
        s = r.smembers(const.STATION_SET_KEY)
        for c in s:
            from_station_id = bytes.decode(c)
            key = gen_key(const.STREAM_FROM_HEAD, from_station_id)
            res[from_station_id] = r.hgetall(key)
        return json.dumps(res)
    elif to_station_id == '':
        key = gen_key(const.STREAM_FROM_HEAD, from_station_id)
        return json.dumps({from_station_id: r.hgetall(key)})
    else:
        key = gen_key(const.STREAM_FROM_HEAD, from_station_id)
        return json.dumps({from_station_id: {to_station_id: r.hget(key, to_station_id)}})

if __name__ == "__main__":
    app.run()