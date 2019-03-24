from db_models import database
import subprocess
import os
import shutil
from utils.logs import add_to_last_log

tmp_dir = os.getcwd() + "/tmp"


def clean_tmp(dir_path):
    """
    get a folder path and try to remove all files
    :param dir_path:
    :return:
    """
    try:
        shutil.rmtree(dir_path)
    except Exception as e:
        pass


def _cli(cmd, without_output=False, read_from_stdout=False):
    if without_output:
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
    else:
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True, bufsize=10 ** 8,
            universal_newlines=True
        )
        if read_from_stdout:
            return p.stdout.read()
        return p.stderr.read()


def convert_to_utf8(file_name):

    print("FILE ENCODE FIX :", file_name)
    add_to_last_log("FILE ENCODE FIX :" + file_name)

    if os.path.isfile(file_name + "_new"):
        os.remove(file_name + "_new")

    qstr = "iconv -c -t utf-8//TRANSLIT "+file_name + " > " + file_name + "_new"
    _cli(qstr)

    print("FILE ENCODE FIXED :", file_name)
    add_to_last_log("FILE ENCODE FIXED :" + file_name)


def move_to_postgres(file_name, table, fields):
    print("FILE IMPORT :", file_name)
    add_to_last_log("FILE IMPORT :" + file_name)

    query_str = "TRUNCATE table " + table + ";" + \
                "SET client_encoding to 'UTF8';" + \
                "update pg_database set encoding = pg_char_to_encoding('UTF8') where datname = 'keywords';" + \
                "COPY " + table + "(" + fields + ") FROM '" + file_name +\
                "' DELIMITER ',' CSV HEADER NULL 'NA' ENCODING 'UTF8';"

    print(query_str)
    q = database.execute_sql(sql=query_str)
    print("FILE IMPORTED :", file_name)
    add_to_last_log("FILE IMPORTED :" + file_name)


def download_from_kaggle():
    os.environ['KAGGLE_USERNAME'] = 'mirzakhani'
    os.environ['KAGGLE_KEY'] = '74f668ae98024327bbd8ed40fe4c2c70'

    print("START DOWNLOAD FROM KAGGLE")
    add_to_last_log("START DOWNLOAD FROM KAGGLE")

    clean_tmp(tmp_dir)
    _cli("kaggle datasets download -d stackoverflow/pythonquestions -p " + tmp_dir)

    print("DOWNLOAD COMPLETE")
    add_to_last_log("DOWNLOAD COMPLETE")


def load_from_csv_into_psql():
    add_to_last_log("--------FETCH LAST DATA FROM KAGGLE--------")
    download_from_kaggle()

    qfile = tmp_dir + "/Questions.csv"
    afile = tmp_dir + "/Answers.csv"
    tfile = tmp_dir + "/Tags.csv"

    qfields = "id,owneruserid,creationdate,score,title,body"
    afields = "id,owneruserid,creationdate,parentid,score,body"

    convert_to_utf8(qfile)
    convert_to_utf8(afile)
    convert_to_utf8(tfile)

    move_to_postgres(qfile + "_new", "questions", qfields)
    move_to_postgres(afile + "_new", "answers", afields)
    move_to_postgres(tfile + "_new", "tags", "id,tag")

    add_to_last_log("--------FETCH DATA FROM KAGGLE FINISHED--------")


if __name__ == "__main__":
    load_from_csv_into_psql()

