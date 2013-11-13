from flask import Flask, request
from flask.ext.uploads import *
import dbmerge
import analyze
import db2csv
import glob
import tasks
app = Flask(__name__)
app.config.update(
        USER_OPTIONS=(),
            CELERY_BROKER_URL='redis://localhost:6379',
                CELERY_RESULT_BACKEND='redis://localhost:6379'
                )
#celery = tasks.make_celery(app)

@app.route('/')
def hello_world():
    return 'HELLO'

@app.route('/data', methods=['GET', 'POST'])
def upload():
    return 200
@app.route('/data', methods=['GET', 'POST'])
def upload_data():
    #if request.method == 'GET':

    try:
        os.system("analyze.py "+"/var/uploads/processed/done/ScreenProbe.csv " +"/var/uploads/test.csv")
        return 'Script ran!'
        #analyze.run_analysis("/var/uploads/processed/done/ScreenProbe.csv", "/var/uploads/test.csv")
    except:
        return "OH NO"
    #convert_file("/var/uploads/processed/done/ScreenProbe.csv", "/var/uploads/test.csv")

    #import glob
    return "200"
    #else:
        #return 'hey there!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10080)
