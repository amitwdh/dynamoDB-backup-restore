import argparse
import boto3
import logging
import restore_dynamodb
import sys
import time
import utils
from datetime import datetime, timezone, timedelta
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_args_parser():
    parser = argparse.ArgumentParser(description="Utility to test restore DynamoDB for given client")
    parser.add_argument("--source-client-name", dest="source_client_name", action="store", default='test-pitr')
    parser.add_argument("--target-client-name", dest="target_client_name", action="store", default='test-pitr-restored')
    parser.add_argument("--aws-profile", dest="aws_profile", action="store", default='default')
    parser.add_argument("--env", dest="env", action="store", default='staging')
    return parser

def create_tables(table_name, aws_profile):
    session = boto3.Session(profile_name=aws_profile)
    dest_dynamo_client = session.client('dynamodb')
    logging.info("Dynamo DB table %s creation in progress" %table_name)
    dest_dynamo_client.create_table(TableName = table_name, AttributeDefinitions=[{'AttributeName': 'name', 'AttributeType': 'S'}], KeySchema=[{'AttributeName': 'name', 'KeyType': 'HASH'}], BillingMode="PAY_PER_REQUEST")
    waiter = dest_dynamo_client.get_waiter('table_exists')
    waiter.wait(TableName=table_name)
    time.sleep(5)
    logging.info("Dynamo DB table %s created successfully" %table_name)


def insert_N_records_every_2_seconds_and_return_kth_record_time(table_name, n, k, aws_profile):
    count = 1
    time_for_kth_record = datetime.now(tz=timezone.utc)
    while count < n:
        session = boto3.Session(profile_name=aws_profile)
        table = session.resource('dynamodb').Table(table_name)
        current_time = datetime.now(tz=timezone.utc)
        if count == k:
            time_for_kth_record = current_time
        nameValue = "record" + str(count)
        lastUpdatedTimeValue = str(current_time).replace(" ", "T").replace("+00:00", "Z")
        response = table.put_item(Item={
            'name': nameValue,
            'lastUpdatedTime': lastUpdatedTimeValue
        })
        logging.info("Inserted name: %s and lastUpdatedTime: %s", nameValue, lastUpdatedTimeValue)
        count = count + 1
        time.sleep(2)
    logging.info("Inserted %s records to %s table", count-1, table_name)
    return time_for_kth_record


def print_table_records(table_name):
    logging.info("Printing recovered table %s records", table_name)
    session = boto3.Session(profile_name=args.aws_profile)
    client = session.client('dynamodb')
    dynamopaginator = client.get_paginator('scan')
    dynamoresponse = dynamopaginator.paginate(
        TableName = table_name,
        Select='ALL_ATTRIBUTES',
        ReturnConsumedCapacity='NONE',
        ConsistentRead=True
    )
    for page in dynamoresponse:
        for item in page['Items']:
            logging.info(item)


def verify_table_records(expected_records, table_name):
    logging.info("Verifying recovered table %s records. Expected are: %s", table_name, expected_records)
    session = boto3.Session(profile_name=args.aws_profile)
    client = session.client('dynamodb')
    dynamopaginator = client.get_paginator('scan')
    dynamoresponse = dynamopaginator.paginate(
        TableName = table_name,
        Select='ALL_ATTRIBUTES',
        ReturnConsumedCapacity='NONE',
        ConsistentRead=True
    )
    for page in dynamoresponse:
        all_match = True
        for item in page['Items']:
            logging.info(item)
            if (item.get('name').get('S') in expected_records):
                logging.info("Record %s present in expected records list", item)
            else:
                logging.info("Record %s NOT present in expected records list", item)
                all_match = False
    if all_match == False:
        raise Exception("Expected and Actual records list not matching")

    return all_match

def delete_create_and_enable_pitr_on_table(source_table, target_table, aws_profile):
    restore_dynamodb.delete_table_if_exist(source_table, args.aws_profile)
    restore_dynamodb.delete_table_if_exist(target_table, args.aws_profile)
    create_tables(source_table, aws_profile)
    restore_dynamodb.enable_point_in_time_recovery_on_table(source_table, aws_profile)

def test_recovery_point_after_earliest_restorable_point(args):
    try:
        logging.info("############-test_recovery_point_AFTER_earliest_restorable_point-#############")
        source_table = utils.get_lro_store_table_name(args.source_client_name, args.env)
        target_table = utils.get_lro_store_table_name(args.target_client_name, args.env)
        delete_create_and_enable_pitr_on_table(source_table, target_table, args.aws_profile)
        datetime_of_5th_record = insert_N_records_every_2_seconds_and_return_kth_record_time(source_table, 10, 5, args.aws_profile)
        logging.info("5th record time: %s", datetime_of_5th_record)
        recovery_point_datetime = datetime_of_5th_record - timedelta(seconds=1)
        recovery_point_str = recovery_point_datetime.strftime('%Y-%m-%d %H:%M:%S')
        logging.info("Recovery point time: %s", recovery_point_str)
        restore_dynamodb.recover_lro_store(args.source_client_name, args.target_client_name, recovery_point_str, args.env, args.aws_profile)
        print_table_records(target_table)
        expected_records = ["record1", "record2", "record3", "record4"]
        matched = verify_table_records(expected_records, target_table)
        if(matched):
            logging.info("Test Successful!")
            return 0
    except Exception:
        logger.info("Test Failed!")
        return 1


def test_recovery_point_before_earliest_restorable_point(args):
    try:
        logging.info("############-test_recovery_point_BEFORE_earliest_restorable_point-#############")
        source_table = utils.get_lro_store_table_name(args.source_client_name, args.env)
        target_table = utils.get_lro_store_table_name(args.target_client_name, args.env)
        delete_create_and_enable_pitr_on_table(source_table, target_table, args.aws_profile)
        datetime_of_3rd_record = insert_N_records_every_2_seconds_and_return_kth_record_time(source_table, 10, 3, args.aws_profile)
        logging.info("3rd record time: %s", datetime_of_3rd_record)
        recovery_point_datetime = datetime_of_3rd_record - timedelta(seconds=0)
        recovery_point_str = recovery_point_datetime.strftime('%Y-%m-%d %H:%M:%S')
        logging.info("Recovery point time: %s", recovery_point_str)
        restore_dynamodb.recover_lro_store(args.source_client_name, args.target_client_name, recovery_point_str, args.env, args.aws_profile, 10)
        print_table_records(target_table)
        expected_records = ["record1", "record2"]
        matched = verify_table_records(expected_records, target_table)
        if(matched):
            logging.info("Test Successful!")
            return 0
    except Exception:
        logger.info("Test Failed!")
        return 1
    pass

def test_source_table_does_not_exist(args):
    try:
        logging.info("############-test_source_table_does_not_exist-#############")
        return restore_dynamodb.recover_lro_store("args.source_client_name", args.target_client_name, "", args.env, args.aws_profile, 10)
    except Exception:
        logging.info("Test Failed!")
        return 1
    pass

def test_source_table_pitr_disabled(args):
    try:
        logging.info("############-test_source_table_pitr_disabled-#############")
        source_table = utils.get_lro_store_table_name(args.source_client_name, args.env)
        restore_dynamodb.delete_table_if_exist(source_table, args.aws_profile)
        create_tables(source_table, args.aws_profile)
        return restore_dynamodb.recover_lro_store(args.source_client_name, args.target_client_name, "", args.env, args.aws_profile, 10)
    except Exception:
        logging.info("Test Failed!")
        return 1
    pass


def log_test_status_successful_if_0(status, test_name):
    if status == 1:
        logging.info("%s FAILED", test_name)
    else:
        logging.info("%s SUCCESSFUL", test_name)
    pass

def log_test_status_successful_if_1(status, test_name):
    if status == 1:
        logging.info("%s SUCCESSFUL", test_name)
    else:
        logging.info("%s FAILED", test_name)
    pass


if __name__ == "__main__":
    args = get_args_parser().parse_args()
    #restore_dynamodb.enable_point_in_time_recovery_on_table("adapter-lro-store-client-wadhe-test-3-staging", args.aws_profile)

    test_1_status = test_recovery_point_after_earliest_restorable_point(args)
    test_2_status = test_recovery_point_before_earliest_restorable_point(args)
    test_3_status = test_source_table_does_not_exist(args)
    test_4_status = test_source_table_pitr_disabled(args)

    log_test_status_successful_if_0(test_1_status, "test_recovery_point_after_earliest_restorable_point")
    log_test_status_successful_if_0(test_2_status, "test_recovery_point_before_earliest_restorable_point")
    log_test_status_successful_if_1(test_3_status, "test_source_table_does_not_exist")
    log_test_status_successful_if_1(test_4_status, "test_source_table_pitr_disabled")

    if test_1_status == 0 and test_2_status == 0 and test_3_status == 1 and test_4_status == 1:
        logging.info("Test Case Succeeded!")
        sys.exit(0)
    else:
        raise Exception("Test Case Failed!")






