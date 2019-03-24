import pymongo
from pymongo import MongoClient
from bs4 import BeautifulSoup
from utils.logs import add_to_last_log


def separate_codes():
    add_to_last_log("--------START SEPERATE CODES--------")
    client = MongoClient('mongodb://localhost:27017/')
    mongodb = client.anl_database

    questions = mongodb.questions.find({})

    i = 1
    n = 1
    j = questions.count()
    for question in questions:

        qcode_str = BeautifulSoup(question["body"], 'html.parser').find('code')
        if qcode_str is None:
            continue
        question_code = qcode_str.text.strip()
        res = mongodb.questions.update_one(
            filter={"_id": question["_id"]},
            update={
                "$set": {"question_code": question_code}
            },
            upsert=True
        )

        # print("process {} of {}".format(i, j))
        i += 1

        if i > 1000:
            n += 1
            i = 0
            print("process {} of {}".format(n, j))
            add_to_last_log("--------PROCESS RECORD {} OF {}".format(n, j))
    add_to_last_log("--------SEPERATE CODES FINISHED--------")


if __name__ == "__main__":
    separate_codes()