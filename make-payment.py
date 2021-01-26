#!/usr/bin/env python3
##
# Simulates potential write skew conflicts that can occur for MongoDB, as with any other database
# which provides 'snapshot isolation'.
##
import argparse
import time
from decimal import Decimal
from bson.decimal128 import Decimal128
from pymongo import MongoClient
from pymongo import ReturnDocument
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern


####
# Main
####
def main():
    argparser = argparse.ArgumentParser(description='Write skew simulator based on performing bank '
                                                    'transfer payments')
    argparser.add_argument('-u', '--url',
                           help=f'MongoDB Cluster URL (default: {DEFAULT_MONGODB_URL})',
                           default=DEFAULT_MONGODB_URL)
    argparser.add_argument('-p', '--payment',
                           help=f'Payment amount - positive integer (default: {DEFAULT_PAYMENT})',
                           default=DEFAULT_PAYMENT,
                           type=int)
    argparser.add_argument('-a', '--account_type',
                           help=f'Account type',
                           choices=ACCOUNT_TYPE_CHOICES,
                           required=True)
    argparser.add_argument('-b', '--behaviour',
                           help=f'Beheviour (default: {DEFAULT_BEHAVIOUR})',
                           default=DEFAULT_BEHAVIOUR,
                           choices=BEHAVIOUR_CHOICES)
    args = argparser.parse_args()
    print()
    title = f"Payment attempt from Alice to Bob for amount '{args.payment}', for account types " \
            f"'{args.account_type}'"
    print(f'Started: {title}')
    make_payment(args.url, args.payment, args.account_type,
                 args.behaviour == DO_CONFLICT_CHECK_BEHAVIOUR)
    print(f'Finished: {title}')
    print()


####
# Perform the payment from one of Alice's bank accounts to one of Bob's bank accounts.
####
def make_payment(url, payment, account_type, do_conflict_check):
    client = MongoClient(url, readConcernLevel='majority', readPreference='primary')
    acc_coll = client[DB_NAME][ACC_COLL_NAME]

    try:
        with client.start_session() as tx_sess:
            with tx_sess.start_transaction(read_concern=ReadConcern(level='snapshot'),
                                           write_concern=WriteConcern('majority')):
                # READ BALANCES FOR ALICE'S CURRENT & SAVING ACCOUNTS AND TOTAL THEM UP
                aliceBalance = getAliceCurrentBalance(tx_sess, acc_coll, do_conflict_check)

                # STOP PROCESSING THE PAYMENT ALICE DOES NOT HAVE ENOUGH FUNDS
                if (aliceBalance - Decimal(payment)) < 0:
                    print(f" - Correctly refusing payment of '{payment}' because Alice's bank "
                          f"balance is: '{aliceBalance}'")
                    return

                print(f" - Proceeding with payment of '{payment}' because Alice's bank balance is: "
                      f"'{aliceBalance}'")

                # ARTIFICIAL PAUSE TO ENABLE THE RACE CONDITION OF TWO TRANSACTIONS TO OCCUR
                print(f" - Started sleeping for {SLEEP_SECS} seconds")
                time.sleep(SLEEP_SECS)
                print(f" - Finished sleeping")

                # PERFORM THE PAYMENT TRANSFER FROM ONE OF ALICE'S ACCOUNT TO ONE OF BOB'S ACCOUNTS
                acc_coll.update_one(
                    {
                        'account_holder': 'Alice',
                        'account_type': account_type
                    },
                    {
                        '$inc': {'balance': (payment*-1)}
                    },
                    session=tx_sess)
                acc_coll.update_one(
                    {
                        'account_holder': 'Bob',
                        'account_type': account_type
                    },
                    {
                        '$inc': {'balance': payment}
                    },
                    session=tx_sess)
    except Exception as e:
        print(f" - Conflict detected as expected, to prevent a payment of '{payment}' from causing "
              f"Alice to go overdrawn. Exception details: {e}")
        pass

    aliceBalance = getAliceCurrentBalance(None, acc_coll, False)

    if aliceBalance < Decimal(0):
        print(f" - OVERDRAWN ISSUE - Alice's balance overdrawn upon checking after the payment "
              f"attempt completed, with the checked bank balance being: {aliceBalance}")
    else:
        print(f" - Good result - Alice's balance is ok upon checking after the payment attempt "
              f"completed, with the checked bank balance being: {aliceBalance}")


####
# Get Alice's current balance by summing up the current values of each of her accounts
####
def getAliceCurrentBalance(tx_sess, acc_coll, do_conflict_check):
    balance = Decimal(0)

    for record in acc_coll.find({'account_holder': 'Alice'}, session=tx_sess):
        if do_conflict_check:
            account = acc_coll.find_one_and_update(
                {
                    '_id': record['_id']
                },
                {
                    '$set': {'last_check_client_session': tx_sess.session_id}
                },
                return_document=ReturnDocument.AFTER,
                session=tx_sess)
        else:
            account = record

        balance += account['balance'].to_decimal()

    return balance


# Constants
SLEEP_SECS = 10
DB_NAME = 'bank'
ACC_COLL_NAME = 'accounts'
DEFAULT_MONGODB_URL = 'mongodb://localhost:27017,localhost:27027,localhost:27037/?replicaSet=TestRS'
DEFAULT_PAYMENT = 50
ACCOUNT_TYPE_CHOICES = ['CURRENT', 'SAVINGS']
DEFAULT_BEHAVIOUR = 'NO_CONFLICT_CHECK'
DO_CONFLICT_CHECK_BEHAVIOUR = 'DO_CONFLICT_CHECK'
BEHAVIOUR_CHOICES = [DEFAULT_BEHAVIOUR, DO_CONFLICT_CHECK_BEHAVIOUR]


####
# Start-up
####
if __name__ == '__main__':
    main()
