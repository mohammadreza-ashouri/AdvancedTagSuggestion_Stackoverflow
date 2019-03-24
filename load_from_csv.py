import argparse
from pymongo import MongoClient
import csv
from bs4 import BeautifulSoup
from queue import Queue
import threading

QUEUE = Queue()
WORKERS = 20

questions_list = {}


def soup_worker():
    while True:
        if not QUEUE.empty():
            qitem = QUEUE.get()
            qcode_str = BeautifulSoup(qitem["body"], 'html.parser').find('code')
            if qcode_str is None:
                continue
            question_code = qcode_str.text.strip()
            qitem["question_code"] = question_code

            questions_list[str(qitem[0])] = qitem
            QUEUE.task_done()


def load_questions(qfile):

    first_row = True
    with open(qfile, 'r', encoding="ISO-8859-1") as questions:
        qitems = csv.reader(questions)

        for item in qitems:
            if first_row:
                first_row = False
                continue

            q_item = {
                "question_id": item[0],
                "user_id": item[1],
                "create_at": item[2],
                "score": item[3],
                "title": item[4],
                "body": item[5]
            }

            QUEUE.put(q_item)

            questions_list[str(item[0])] = q_item

    for w in range(WORKERS):
        t = threading.Thread(target=soup_worker, name='soup_worker-%s' % w)
        t.daemon = True
        t.start()

    QUEUE.join()

    return questions_list


def load_answers(afile):
    answers_list = {}
    first_row = True
    with open(afile, 'r', encoding="ISO-8859-1") as answers:
        aitems = csv.reader(answers)

        for item in aitems:
            if first_row:
                first_row = False
                continue

            a_item = {
                "answer_id": item[0],
                "user_id": item[1],
                "create_at": item[2],
                "parentid": item[3],
                "score": item[4],
                "body": item[5]
            }
            answers_list[item[3] + "_" + str(item[0])] = a_item
    return answers_list


def load_tags(tfile):
    tags_list = {}
    first_row = True
    with open(tfile, 'r', encoding="ISO-8859-1") as tags:
        titems = csv.reader(tags)

        i = 0
        for item in titems:
            if first_row:
                first_row = False
                continue

            t_item = {
                "id": item[0],
                "tag": item[1],
            }
            tags_list[str(item[0])+"_"+str(i)] = t_item

            i = i + 1
    return tags_list


def write_to_mongodb(db, data):

    res = db.questions.insert(
        data, ordered=False)


def send_to_mongodb(qfile, afile, tfile):
    client = MongoClient('mongodb://localhost:27017/')
    mongodb = client.anl_database_new

    questions_list = load_questions(qfile)
    print("question files read complete")

    answers_list = load_answers(afile)
    print("answers files read complete")

    tags_list = load_tags(tfile)
    print("tags files read complete")

    print("processing data")


    to_append = []
    appned_counter = 0
    for key, value in questions_list.iteritems():

        question = value.copy()

        answers = [
            (key, value) for key, value in answers_list.iteritems() if key.startswith(str(question["question_id"]))
        ]

        question["answers"] = answers

        tags = [
            (key, value) for key, value in tags_list.iteritems() if key.startswith(str(question["question_id"]))
        ]

        question["tags"] = tags

        if appned_counter < 1000:

            to_append.append(question)

        else:
            write_to_mongodb(mongodb, to_append)
            appned_counter = 0
            to_append = []

    write_to_mongodb(mongodb, to_append)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("questionfile", help="question file")
    parser.add_argument("answerfile", help="answer file")
    parser.add_argument("tagsfile", help="tags file")
    args = parser.parse_args()

    qfile = args.questionfile
    afile = args.answerfile
    tfile = args.tagsfile

    send_to_mongodb(qfile, afile, tfile)


if __name__ == '__main__':

    main()
