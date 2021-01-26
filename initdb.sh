#!/bin/bash

if [ -z "$1" ]; then
    echo "No MongoDB cluster URL argument provided"
    exit 1
fi

DB_NAME="bank"
ACC_COLL_NAME="accounts"
DB_URL="$1"
printf "\nUsing MongoDB DB URL: ${DB_URL}\n"

printf "\nStarted: Initialising DB\n"
mongo --eval "
    db = db.getSiblingDB('${DB_NAME}');
    accColl = db['${ACC_COLL_NAME}'];
    accColl.drop();    
    accColl.createIndex({account_holder: 1, account_type: 1}, {unique: true});
    print('Re-created database and indexes with empty collections');
" ${DB_URL}

mongoimport --uri ${DB_URL} -d ${DB_NAME} -c ${ACC_COLL_NAME} --type csv --headerline --useArrayIndexFields --columnsHaveTypes "${ACC_COLL_NAME}.csv"
printf "Imported data into DB collections\n"

printf "Finished: Initialising DB\n"
printf "CONNECTION URL:\n"
printf "mongo \"${DB_URL}\"\n\n"

