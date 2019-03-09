# -*- coding: utf-8 -*-
"""
Created on Fri Sep 15 11:00:22 2017

@author: Administrator
"""
from flask import Flask, request, render_template, jsonify
from urllib import parse
import main_qa
application1 = Flask(__name__)

robot = main_qa.Robots()

@application1.route("/")
def api_index():
    return render_template('index.html')

@application1.route('/get_answer',methods=['POST'])
def get_answer():
    que=parse.parse_qs(request.get_data().decode('utf-8'))
    print(que['text'][0],"11111")
    resu = robot.get_answer_qa(que['text'][0])
    print(resu,"3333333333333333333333333333333333")
    return jsonify({"key":resu})
if __name__ == "__main__":
    application1.run('127.0.0.1', 6550,debug=True)



