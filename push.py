import os
import sys
import json
import redis
from deeputils.common import log, dict_merge

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python3 db.py <path>')
    else:
        # Init database session
        rds = redis.Redis()
        # Load data into database
        path = sys.argv[1]
        for rt, dirs, files in os.walk(path):
            for f in files:
                file_name = os.path.splitext(f)
                if str(file_name[1]) == '.json':
                    file_path = os.path.join(rt, f)
                    print(file_path)
                    fp = file_path.split('.')[1].split('/')[2:]
                    topic = 'ohlc-{}-{}-{}-{}-{}'.format(fp[1], 'spot', fp[3], 'm1', fp[4])
                    log('Loading data from {}, topic: {}'.format(file_path, topic))
                    data_old = rds.get(topic)
                    data_old = json.loads(data_old) if data_old is not None else list()
                    with open(file_path, 'r') as ff:
                        data_new = json.loads(ff.read())
                        data_submit = dict_merge(data_old, data_new, 't')
                        rds.set(topic, data_submit)
