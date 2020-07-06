import pymysql
import json
import os

def connectdb(autocommit: bool = True):
    """connect to the db using pymysql
    args:
        autocommit: bool
            arg for pymysql.connect
    returns
        db: obj
            pymysql database object
        cur: obj
            cursor object
    raises:
        FileNotFoundError: depends on ~/jbq_credentials.json
    """
    with open(os.path.expanduser('jbq_credentials.json')) as f:
        credentials = json.load(f)
    if credentials:
        print("connecting to database..\n", credentials)
        db = pymysql.connect(host=credentials['host'],  # your host
                             user=credentials['user'],  # your username
                             passwd=credentials['password'],  # your password
                             db=credentials['db'],  # name of the database
                             autocommit=autocommit)
        cur = db.cursor()
        print("successfully connected..")
        return db, cur
    else:
        print("could not read ~/jbq_credentials.json")
        raise FileNotFoundError


def closedb(db):
    """close the db connection"""
    print("committing DB just in case i forgot..")
    try:
        db.commit()
    except:
        # don't care about error
        pass
    print("closing connection to database..")
    try:
        if not db._closed:
            db.close()
            print("Closed db")
    except Exception as e:
        print("Exception when closing db", e)
        pass


def validate_params(params):
    """assert that json contains all required params"""
    keys = params.keys()
    try:
        assert params["artist"] in keys, 'An artist must be provided'
        assert params["genre"] in keys, 'A Genre must be provided'
        assert params["lyrics"] in keys, 'Lyrics must be provided'
        return True
    except AssertionError:
        return False


def parse_params(row):
    """takes mysql row dict, coverts params from json-formatted string into dict"""
    # attempt to parse params as a json
    row["params"] = json.loads(row["params"])
    # assert the json is in right format
    assert validate_params(row["params"]), "The JSON format is not valid"


def get_next_job(cur, status="top_ready"):
    """select a random new job which is unlocked and has a particular status"""
    query = 'SELECT * from jobs_jukebox \
                where locked=0 and status=%s \
                ORDER BY rand() limit 1'
    cur.execute(query, status)
    fields = [f[0] for f in cur.description]
    rows = cur.fetchone()
    if not rows:
        print("no jobs available")
        return None
    row = dict(zip(fields, rows))
    parse_params(row)
    return row


def get_job(cur, job_id: str):
    """retrieve a specific job
    args:
        cur: obj
            pysql db.cursor() object
        job_id: str
            unique id of the job to retrieve
    """
    query = 'select * from jobs_jukebox where job_id=%s '
    cur.execute(query, job_id)
    fields = [f[0] for f in cur.description]
    rows = cur.fetchone()
    if not rows:
        print("unknown job with id", job_id)
        return None
    row = dict(zip(fields, rows))
    parse_params(row)
    return row


def new_job(cur, name: str, params: dict, status: str = "top_ready"):
    """create a new job
    args:
        cur: obj
            pysql db.cursor() object
        name: str
            The desired name of the job
        params: dict
            JSON configuration of jukebox hyper-parameters and conditioning info
        status: str
            "top_ready" if it's a totally new job, else "upsampling_ready" if the TopTier was already generated
    """
    # assert the json is in right format
    assert validate_params(params)
    params = json.dumps(params)
    query = 'insert into jobs_jukebox (name, locked, params, status) values(%s,0,%s,%s)'
    values = (name, params, status)
    cur.execute(query, values)
    job_id = cur.lastrowid
    print(job_id)
    # ok now return the job dict
    return get_job(cur, job_id)


def lock(cur, job_id: str):
    """locking prevents this job from being taken by another process"""
    query = 'update jobs_jukebox set locked=1 where job_id=%s '
    cur.execute(query, job_id)


def unlock(cur, job_id):
    """unlock to free this job up"""
    query = 'update jobs_jukebox set locked=0 where job_id=%s '
    cur.execute(query, job_id)


def update_status(cur, job_id: str, status: str):
    """update this job's status"""
    query = 'update jobs_jukebox set status=%s where job_id=%s '
    cur.execute(query, (status, job_id))


def log(cur, job_id, _log):
    """appends log information to the job
    for example, the URL, or errors"""
    query = "UPDATE jobs_jukebox SET log = CONCAT(COALESCE(log,''), %s) where job_id=%s"
    _log = str(_log) + "\n"
    cur.execute(query, (_log, job_id))
