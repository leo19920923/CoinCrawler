import os
import sys
import redis
from deeputils.common import log

def push(path):
    pass


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
                    with open(file_path, 'r') as ff:
                        rds.set(topic, ff.read())