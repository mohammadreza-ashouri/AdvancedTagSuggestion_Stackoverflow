from peewee import *
from db_models import Tags, Questions, Answers, database
import pymongo
from pymongo import MongoClient
from utils.logs import add_to_last_log


def move_to_mongodb():
    add_to_last_log("--------START MOVE DATA TO MONGODB--------")
    client = MongoClient('mongodb://localhost:27017/')
    mongodb = client.anl_database

    # mongodb.questions.remove({})

    sqlq = """
        select
          q.id as qid,
          a.id as aid,
          q.creationdate as qdate,
          a.creationdate as adate,
          q.title as qtitle,
          q.body as qbody,
          a.body as abody,
          a.score as ascore,
          q.score as qscore,
          t.id as tid,
          t.tag as tag,
          q.owneruserid as quser_id,
          a.owneruserid as auser_id
        from questions as q
        left join answers as a on q.id = a.parentid
        left join tags as t on q.id = t.id
        limit 1000
    """
    i = 1
    q = database.execute_sql(sql=sqlq)
    for a in q:

        add_to_last_log("--------PROCESS RECORD NO:" + str(i) + " -----")
        i = i+1
        question_obj = {
            "question_id": a[0],
            "title": a[4],
            "body": a[5],
            "score": a[8],
            "user_id": a[11],
            "create_at": a[2]
        }

        answer_obj = {
            "answer_id": a[1],
            "body": a[6],
            "score": a[7],
            "user_id": a[12],
            "question_id": a[0],
            "create_at": a[3]
        }

        tag_obj = {
            "question_id": a[0],
            "tag": a[10]
        }

        q_obj = mongodb.questions.find_one(
            {"question_id": question_obj["question_id"]}
        )

        if q_obj is None:

            question_obj["answers"] = []
            question_obj["answers"].append(answer_obj)

            question_obj["tags"] = []
            question_obj["tags"].append(tag_obj)

            res = mongodb.questions.insert_one(
                question_obj).inserted_id

        else:

            if answer_obj not in q_obj["answers"]:
                q_obj["answers"].append(answer_obj)

            if tag_obj not in q_obj["tags"]:
                q_obj["tags"].append(tag_obj)

            ins_data = q_obj.copy()
            del ins_data["_id"]

            res = mongodb.questions.update_one(
                filter={"_id": q_obj["_id"]},
                update={
                    "$set": ins_data
                },
                upsert=True
            )
    add_to_last_log("--------MOVE DATA TO MONGODB FINISHED--------")


if __name__ == "__main__":

    move_to_mongodb()
