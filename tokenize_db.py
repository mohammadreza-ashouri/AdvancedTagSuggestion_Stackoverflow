from pymongo import MongoClient
from tokenize import tokenize, untokenize, NUMBER, STRING, NAME, OP
from io import BytesIO
import keyword
from utils.logs import add_to_last_log


def tokenize_db():
    add_to_last_log("--------START TOKENIZE CODES--------")
    client = MongoClient('mongodb://localhost:27017/')
    mongodb = client.anl_database

    questions = mongodb.questions.find({})

    i = 1
    n = 1
    j = questions.count()

    for question in questions:

        if "question_code" in question:

            question_code = question["question_code"]
            tokens = []
            try:
                g = tokenize(BytesIO(question_code.encode('utf-8')).readline)
                for toknum, tokval, _, _, _ in g:
                    if toknum == NAME and not keyword.iskeyword(tokval):
                        if tokval not in tokens:
                            tokens.append(tokval)
            except:
                continue

            res = mongodb.questions.update_one(
                filter={"_id": question["_id"]},
                update={
                    "$set": {"question_tokens": tokens}
                },
                upsert=True
            )

        i += 1

        if i > 1000:
            n += 1
            i = 0
            print("process {} of {}".format(n, j))
            add_to_last_log("--------PROCESS RECORD {} OF {}".format(n, j))
    add_to_last_log("--------TOKENIZE CODES FINISHED--------")


if __name__ == "__main__":
    tokenize_db()
