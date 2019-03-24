from peewee import *

database = PostgresqlDatabase('keywords', user='mohsen', password='dokaven1367',
                           host='localhost', port=5432)


class UnknownField(object):
    def __init__(self, *_, **__): pass


class BaseModel(Model):
    class Meta:
        database = database


class Questions(BaseModel):
    body = TextField(null=True)
    creationdate = TextField(null=True)
    id = IntegerField(null=True)
    owneruserid = IntegerField(null=True)
    score = IntegerField(null=True)
    title = TextField(null=True)

    class Meta:
        table_name = 'questions'
        primary_key = False


class Answers(BaseModel):
    body = TextField(null=True)
    creationdate = TextField(null=True)
    id = IntegerField(null=True)
    owneruserid = IntegerField(null=True)
    parentid = IntegerField(null=True)
    score = IntegerField(null=True)

    class Meta:
        table_name = 'answers'
        primary_key = False


class Tags(BaseModel):
    id = IntegerField(null=True)
    tag = CharField(null=True)

    class Meta:
        table_name = 'tags'
        primary_key = False

