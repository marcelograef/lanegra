from flask import Flask, redirect, url_for, render_template, request
import sys
import subprocess
from datetime import datetime
import api_requests
import functions
"""
from bd import conexion

cursor = conexion.cursor()
 """
app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/run_script", methods=['GET', 'POST'])
def run_script():
    data = request.form

    fecha_inicio = data['report-start']
    fecha_fin = data['report-finish']

    id_lote = functions.getLoteKey('Carga_Int_Dim_Py')
    functions.insertProcessKey(id_lote, fecha_inicio, fecha_fin)
    theproc = subprocess.Popen(['python', 'carga_int.py'])
    output = theproc.communicate()
    return render_template("template1.html")


if __name__ == "__main__":
    app.run(debug=True)
