from flask import render_template, request, redirect, url_for, session
from app import app, mongodb
from utils.modules import (
    load_module_list,
    remove_module,
    get_module_detail
)
import threading
from utils.logs import (
    load_logs_list,
    get_log_item,
    insert_log
)
import re
from utils.analytic import get_text_keyword_with_scores,get_code_keywords
from bson.objectid import ObjectId
import keyword
from datetime import datetime

from load_from_csv_pg import load_from_csv_into_psql
from move_to_mongo import move_to_mongodb
from separate_codes import separate_codes
from tokenize_db import tokenize_db

import Levenshtein


def import_data_thread():
    with app.app_context():
        load_from_csv_into_psql()
        move_to_mongodb()
        separate_codes()
        tokenize_db()


def get_answers(keywords, code):

    result_list = []
    data = mongodb.db.questions.find()

    for d in data:

        if "question_code" in d:
            try:
                # res = pycode_similar.detect([code, d["question_code"]], diff_method=UnifiedDiff)
                r = Levenshtein.ratio(code, d["question_code"])
                result_list.append({
                    "percent": r,
                    "question": d,
                })
            except Exception as e:
                print(e)
                pass

    newlist = sorted(result_list, key=lambda k: k['percent'], reverse=True)
    return newlist


def get_answers_tags(tags):
    data = mongodb.db.questions.aggregate([
       {"$match": {"question_tokens": {"$in": tags}}},
       {"$project": {
           "tagMatch": {
               "$setIntersection": [
                   "$question_tokens",
                   tags
               ]
           },
           "sizeMatch": {
               "$size": {
                   "$setIntersection": [
                       "$question_tokens",
                       tags
                   ]
               }
           },
           "title": 1,
           "body": 1,
           "answers": 1
       }},
       {"$match": {"sizeMatch": {"$gte": 1}}},
       {"$project": {"tagMatch": 1, "title": 1, "body": 1, "answers": 1, "sizeMatch": 1}},
       {"$sort": {"sizeMatch": -1}},
       {"$limit": 10}
    ])

    similar_tags = []
    result = [a for a in data]

    for item in result:
        for tag_item in item["tagMatch"]:
            similar_tags.append(tag_item)

    final_code_tags = list(set(similar_tags))

    final_code_tags = [a.lower() for a in final_code_tags]

    title_tags = []

    for result_item in result:

        tags = get_text_keyword_with_scores(result_item["title"])

        for t in tags:
            clean_string = re.sub('\W+', ' ', str(t[1]))
            f = clean_string.split(" ")
            title_tags = title_tags + f

    title_tags = list(set(title_tags))

    title_tags = [a.lower() for a in title_tags if len(a) > 1 and not str(a).isnumeric()]

    final_tags = list(set(final_code_tags).intersection(title_tags))
    # final_tags = []

    # for final_code_tag in final_code_tags:
    #     for title_tag in title_tags:
    #
    #         if title_tag in final_code_tag or final_code_tag in title_tag:
    #             final_tags.append(title_tag)
    #
    # final_tags = list(set(final_tags))

    if len(final_tags) > 0:

        return result, final_tags

    return result, final_code_tags


@app.route('/', methods=['GET', 'POST'])
def home_page():
    if request.method == "GET":
        return render_template('index.html')
    elif request.method == "POST":

        question = request.form["question"]
        code = request.form["code"]
        question_keywords = get_text_keyword_with_scores(question)
        code_keywords = get_code_keywords(code)

        session['code_keywords'] = code_keywords
        session['question_keywords'] = question_keywords
        session["question"] = question
        session["code"] = code

        return redirect(url_for("search_result", _external=True))


def clean_tags(keywords):

    t = ""
    for k in keywords:
        t += "," + k

    a = t[1:]

    cleanString = re.sub('\W+', ' ', str(a))

    kw = []
    f = cleanString.split(" ")
    for z in f:

        z = z.strip(" ")
        if len(z) > 1:
            if keyword.iskeyword(z):
                continue
            if z not in kw:
                kw.append(z)

    return kw


@app.route('/result', methods=['GET'])
def search_result():

    if request.method == "GET":
        page = 1
        if "page" in request.args:
            page = int(request.args["page"])

        keywords = session["code_keywords"]
        q_w = session["question_keywords"]
        qu = session["question"]

        code = session["code"]

        # keys = []
        # for q in keywords:
        #     keys.append(q[1])

        qkeys = []
        for q in q_w:
            qkeys.append(q[1])

        # nk = clean_tags(keys)

        # answers = find_answers_with_keywords(kk, page)
        answers, similar_tags = get_answers_tags(tags=keywords)
        return render_template(
            'result.html',
            answers=answers,
            keywords=similar_tags,
            q=qu,
            q_w=qkeys
        )


@app.route('/show/<string:hash_id>', methods=['GET'])
def show_message(hash_id):

    if request.method == "GET":

        question = mongodb.db.questions.find_one({"_id": ObjectId(hash_id)})
        return render_template(
            'show.html',
            question=question,
        )


@app.route('/modules/add', methods=['GET', 'POST'])
def modules_add():

    if request.method == "GET":
        return render_template('add_module.html')
    elif request.method == "POST":

        module_name = request.form["module_name"]
        _, status = get_module_detail(module_name)

        return render_template('add_module.html', status=status)


@app.route('/modules')
def modules_list():
    if request.method == "GET":

        modules = load_module_list()
        return render_template('modules.html', modules=modules)


@app.route('/modules/remove/<string:mid>', methods=['POST'])
def module_remove(mid):
    if request.method == "POST":

        remove_module(mid)
        return redirect(url_for('modules_list'))


@app.route('/start-import', methods=['GET', 'POST'])
def start_import():
    if request.method == "GET":
        return render_template('update_data.html')
    elif request.method == "POST":

        password = request.form["password"]

        if password == "yousef123456":

            insert_log(datetime.now())
            # start_update
            status = "new"

            t1 = threading.Thread(target=import_data_thread, args=[])
            t1.start()

        else:
            status = "wrong_password"

        return render_template('update_data.html', status=status)


@app.route('/logs')
def logs_list():
    if request.method == "GET":
        logs = load_logs_list()
        return render_template('logs.html', logs=logs)


@app.route('/view-log/<string:lid>')
def view_log(lid):

    if request.method == "GET":
        log_item = get_log_item(log_id=lid)

        return render_template('view_log.html', log_item=log_item)


if __name__ == '__main__':
    app.run(debug=True)
