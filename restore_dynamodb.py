import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
import boto3
import sys
import utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_args_parser():
    parser = argparse.ArgumentParser(description="Utility to restore DynamoDB for given client")
    parser.add_argument("--source-client-name", dest="source_client_name", action="store", required=True)
    parser.add_argument("--target-client-name", dest="target_client_name", action="store", required=True)
    parser.add_argument("--restore-datetime", dest="restore_datetime", action="store", help="Format: yyyy-mm-dd hh:mm:ss in UTC", required=True)
    parser.add_argument("--aws-profile", dest="aws_profile", action="store", default='default')
    parser.add_argument("--env", dest="env", action="store", default='staging')
    return parser

def on_demand_backup(source_table_name, source_table_backup_name):
    logging.info("Starting OnDemand backup of " + source_table_name + " table, backup name is %s" %source_table_backup_name)
    backup_output=os.popen("aws dynamodb create-backup --table-name " + source_table_name + " --backup-name " + source_table_backup_name).read()
    logging.info(backup_output)
    backup_details = json.loads(backup_output)
    backup_arn = backup_details["BackupDetails"]["BackupArn"]
    logging.info("%s table backup ARN: %s", source_table_name, backup_arn)
    return backup_arn

def wait_for_backup_to_be_available(backup_name, backup_arn):
    describe_backup_output = os.popen("aws dynamodb describe-backup --backup-arn " + backup_arn).read()
    describe_backup_json = json.loads(describe_backup_output)
    actual_backup_status = describe_backup_json["BackupDescription"]["BackupDetails"]["BackupStatus"]
    expected_backup_status = "AVAILABLE"

    while expected_backup_status != actual_backup_status:
        time.sleep(5)
        describe_backup_output = os.popen("aws dynamodb describe-backup --backup-arn " + backup_arn).read()
        describe_backup_json = json.loads(describe_backup_output)
        actual_backup_status = describe_backup_json["BackupDescription"]["BackupDetails"]["BackupStatus"]
        logging.info("%s backup status is %s", backup_name, actual_backup_status)

    logging.info("OnDemand Backup %s is %s now", backup_name, actual_backup_status)

def restore_table_to_point_in_time(source_table_name, target_table_name, restoration_point):
    logging.info("Starting point in time recovery of table %s to %s", source_table_name, target_table_name)
    output = os.popen("aws dynamodb restore-table-to-point-in-time --source-table-name " + source_table_name + " --target-table-name " + target_table_name + " --restore-date-time " + restoration_point).read()
    logging.info(output)
    # It will poll every 20 seconds until a successful state has been reached. This will exit with a return code of 255 after 25 failed checks.
    table_exist_response = os.popen("aws dynamodb wait table-exists --table-name " + target_table_name).read()
    logging.info(table_exist_response)

def wait_for_table_to_be_in_active_status(table_name):
    describe_table_output = os.popen("aws dynamodb describe-table --table-name " + table_name).read()
    describe_table_output_json = json.loads(describe_table_output)
    actual_table_status = describe_table_output_json["Table"]["TableStatus"]
    expected_table_status = "ACTIVE"

    while expected_table_status != actual_table_status:
        describe_table_output = os.popen("aws dynamodb describe-table --table-name " + table_name).read()
        describe_table_output_json = json.loads(describe_table_output)
        actual_table_status = describe_table_output_json["Table"]["TableStatus"]
        logging.info("%s table status is %s", table_name, actual_table_status)
        time.sleep(5) #5 sec

def delete_table_if_exist(table_name, aws_profile):
    session = boto3.Session(profile_name=aws_profile)
    dynamodb_client = session.client('dynamodb')
    try:
        dynamodb_client.delete_table(TableName=table_name)
        os.system("aws dynamodb wait table-not-exists --table-name " + table_name)
        logging.info("Table %s deleted", table_name)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        logging.info("Table %s doesn't exist", table_name)

def delete_table_using_aws_cli(table_name):
    delete_table_output = os.popen("aws dynamodb delete-table --table-name " + table_name).read()
    logging.info(delete_table_output)
    os.system("aws dynamodb wait table-not-exists --table-name " + table_name)
    logging.info("%s table successfully deleted!" %table_name)

def restore_table_from_backup(target_table_name, backup_arn):
    restore_table_output = os.popen("aws dynamodb restore-table-from-backup --target-table-name " +target_table_name+ " --backup-arn " + backup_arn).read()
    logging.info(restore_table_output)
    os.system("aws dynamodb wait table-exists --table-name " + target_table_name)

def describe_table(table_name):
    os.popen("aws dynamodb describe-table --table-name " + table_name).read()

def is_table_exist(table_name, aws_profile):
    session = boto3.Session(profile_name=aws_profile)
    dynamodb_client = session.client('dynamodb')
    is_table_exist = True
    try:
        dynamodb_client.describe_table(TableName=table_name)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        is_table_exist = False

    if is_table_exist == False:
        logging.info("Source table %s does not exist!", table_name)
        raise Exception("Source table doesn't exist")

def delete_record_by_lro_name(table_name, name):
    try:
        client = boto3.client('dynamodb')
        response = client.delete_item(TableName=table_name, Key={"name": { 'S': name}})
        logging.info("name: %s successfully deleted. Response: %s", name, response)
    except client.ClientError as err:
        logging.info(err)
        sys.exit()
    else:
        return response

def delete_records_with_last_updated_time_after_recovery_point(table_name, recovery_point, aws_profile):
    try:
        session = boto3.Session(profile_name=aws_profile)
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
                if item.get('lastUpdatedTime') == None:
                    continue
                logging.info(item)
                name = item.get('name').get('S')
                last_updated_time_str = item.get('lastUpdatedTime').get('S')
                last_updated_time = datetime.strptime(last_updated_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
                if recovery_point.replace(tzinfo=timezone.utc) < last_updated_time.replace(tzinfo=timezone.utc):
                    logging.info("Last Updated Time %s greater than recovery point %s", last_updated_time, recovery_point)
                    delete_record_by_lro_name(table_name, name)
    except Exception:
        logging.info("Error while deleting records")
        raise Exception("Error while deleting records!")

def log_arguments(source_client_name, target_client_name, aws_profile, restore_datetime_epoch, env):
    logging.info("Restore datetime epoch: %s", restore_datetime_epoch)
    logging.info("Source client Name: %s", source_client_name)
    logging.info("Target client Name: %s", target_client_name)
    logging.info("Aws profile: %s", aws_profile)
    logging.info("Source table name: %s", utils.get_lro_store_table_name(source_client_name, env))
    logging.info("Target table name: %s", utils.get_lro_store_table_name(target_client_name, env))

def enable_point_in_time_recovery_on_table_using_aws_cli(table_name):
    output = os.popen("aws dynamodb update-continuous-backups --table-name " + table_name + " --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true").read()
    logging.info("PITR enabled on table %s, Response: %s", table_name, output)

def enable_point_in_time_recovery_on_table(table_name, aws_profile):
    session = boto3.Session(profile_name=aws_profile)
    client = session.client('dynamodb')
    response = client.update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={
            'PointInTimeRecoveryEnabled': True
        }
    )
    logging.info("PITR enabled on table %s, Response: %s", table_name, response)

def table_arn(table_name, aws_profile):
    session = boto3.Session(profile_name=aws_profile)
    resource = session.resource('dynamodb')
    return resource.Table(table_name).table_arn

def get_pitr_status(table_name, aws_profile):
    session = boto3.Session(profile_name=aws_profile)
    dynamodb_client = session.client('dynamodb')
    continuous_backup_json_response = json.loads(json.dumps(dynamodb_client.describe_continuous_backups(TableName=table_name), default=str))
    pitr_status = continuous_backup_json_response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus']
    logging.info("PointInTimeRecoveryStatus: %s", pitr_status)
    return pitr_status

def get_earliest_restorable_point(table_name, aws_profile):
    session = boto3.Session(profile_name=aws_profile)
    dynamodb_client = session.client('dynamodb')
    continuous_backup_json_response = json.loads(json.dumps(dynamodb_client.describe_continuous_backups(TableName=table_name), default=str))
    earliest_restorable_datetime = continuous_backup_json_response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['EarliestRestorableDateTime']
    logging.info("EarliestRestorableDateTime: %s", earliest_restorable_datetime)
    return earliest_restorable_datetime

def recover_lro_store(source_client, dest_client, restore_datetime, env, aws_profile, seconds_to_add_in_erp = 0):
    try:
        source_table = utils.get_lro_store_table_name(source_client, env)
        target_table = utils.get_lro_store_table_name(dest_client, env)
        is_table_exist(source_table, aws_profile)
        source_table_pitr_status = get_pitr_status(source_table, aws_profile)

        if source_table_pitr_status == "DISABLED":
            logging.info("Source table %s Point in time recovery is DISABLED, so we can't proceed with recovery", source_table)
            raise Exception("Source table PITR is Disabled!")

        restore_datetime = datetime.strptime(restore_datetime, "%Y-%m-%d %H:%M:%S")
        restore_datetime_utc = restore_datetime.replace(tzinfo=timezone.utc)
        log_arguments(source_client, dest_client, aws_profile,  restore_datetime_utc.timestamp(), env)

        earliest_restorable_point = utils.convert_datetime_to_utc_tz(get_earliest_restorable_point(source_table, aws_profile)) + timedelta(seconds=seconds_to_add_in_erp)

        truncate_table_required = False
        if (restore_datetime_utc > earliest_restorable_point):
            logging.info("Recovery point %s is after earliest restorable point %s", restore_datetime_utc, earliest_restorable_point)
            actual_restoration_point = restore_datetime_utc.timestamp()
        else:
            logging.info("Recovery point %s is before earliest restorable point %s", restore_datetime_utc, earliest_restorable_point)
            actual_restoration_point = earliest_restorable_point.timestamp()
            truncate_table_required =True

        delete_table_if_exist(target_table, aws_profile)
        restore_table_to_point_in_time(source_table, target_table, str(actual_restoration_point))
        wait_for_table_to_be_in_active_status(target_table)
        enable_point_in_time_recovery_on_table(target_table, aws_profile)
        if truncate_table_required:
            delete_records_with_last_updated_time_after_recovery_point(target_table, restore_datetime, aws_profile)

        logging.info("Point in Time Recovery is successfully done! in table %s", target_table)
        return 0
    except Exception:
        return 1

def enable_pitr(table_name, aws_profile):
    enable_point_in_time_recovery_on_table(table_name, aws_profile)

if __name__ == "__main__":
    args = get_args_parser().parse_args()
    sys.exit(recover_lro_store(args.source_client_name, args.target_client_name, args.restore_datetime, args.env, args.aws_profile))





        






