from flask import Flask, jsonify
from libs.noaa import NOAA
from libs.nwac import NWAC

app = Flask(__name__)

def update_data():
    noaa = NOAA()
    noaa.update_data()
    nwac = NWAC()
    nwac.update_data()
    return True

@app.route('/api/update_data', methods=['GET'])
def api_update_data():
    return jsonify({'status': 'success'})
