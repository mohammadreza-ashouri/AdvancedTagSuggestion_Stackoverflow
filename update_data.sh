#!/usr/bin/env bash

source /home/question_analytic/env/bin/activate

python load_from_csv_pg.py
python move_to_mongo.py
python separate_codes.py
python tokenize_db.py
