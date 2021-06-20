import yaml
import boto3
from botocore.exceptions import ClientError
import time
import tarfile
import os.path
import logging
logger = logging.getLogger('backup_s3')
logger.setLevel(logging.DEBUG)
from pprint import pprint


def backups_to_s3(config_file="config.yml"):
    ''' iterates a list of dictionaries, tars the source and sends to the s3 bucket target
    parameters
        backups - a list of dictionaries
            backups = [
                {
                    'source': '/full/folder/path',
                    'target': '/s3-bucket/path'
                },
                {
                    'source': '/full/folder/path',
                    'target': '/s3-bucket/path'
                },
            ]
    '''
    setup_logger(config_file)
    
    #get root path for this environment
    #TODO - shoudl we install as environment variable
    root_path = get_root_path_from_config(config_file)

    backups = get_backup_list_from_config(config_file)
    pprint(backups)
    
    for backup in backups:
        pprint(backup)
        if 'backup' in backup:
            if 'src' in backup['backup']:
                tmp_name = backup['backup']['src']
                src_path = f"{root_path}{tmp_name}"
                s3_bucket = backup['backup']['target']['bucket']
                s3_key = backup['backup']['target']['key']
                try:
                    #make a tarfile
                    print(f"making a backup of {tmp_name}")
                    tmp_backup = make_tarfile(tmp_name, src_path)
                    #send it to s3
                    #rename 
                    filename = tmp_backup.split('/')[-1]
                    s3_file_name = f"{s3_key}/{filename}"
                    print(f"uploading {tmp_backup} to {s3_bucket}/{s3_file_name}")
                    resp = send_to_s3(tmp_backup, s3_bucket, s3_file_name)
                    pprint(resp)
                    if resp is True:
                        print(f"removing {tmp_backup}")
                        del_backup(tmp_backup)
                except Exception as err:
                    logger.error(err)
                    pass
                print(src_path)
            print(backup)

def del_backup(filename):
    ''' del_bcakup -- deletes the files
        parameters
            filename: string: the path and name to the file ./backups/file.tar.gz
    '''
    try:
        os.remove(filename) 
    except Exception as err:
        logger.error(err)

def setup_logger(config_file):
    ''' setup_logger -- sets up logging for this application 
    paramaters
        config_file: string: config file (config.yml)
    '''
    log_file = get_log_file_from_config(config_file)
    
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s - line: %(lineno)d - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info('testing logging')

def send_to_s3(file_name, bucket, s3_file):
    ''' send the file to s3 
    parameters - 
        file_name: string : full path of the file to be uploaded
        bucket: string: s3 bucket
        s3_file: string: what the file will be called in s3 (i.e. ./bcakups/foo.tar.gz)
    '''
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        #response = s3_client.upload_file(file_name, bucket, object_name)
        response = s3_client.upload_file(Filename=file_name, Bucket=bucket, Key=s3_file)
        pprint(response)
    except ClientError as e:
        logging.error(e)
        return False
    
    return True

def make_tarfile(output_filename, source_dir):
    ''' make_tarfile -- create a tar file of source_dir with the name of output_filename
        parameters - 
            output_filename: string : filename
            source_dir: string: full path of directory to tar.gz
    '''
    #add './backups/ and datetime to .tar.gz to output_filename
    tmp_now = int(time.time())
    output_filename = f'{output_filename}-{tmp_now}.tar.gz'
    output_filename = f"./backups/{output_filename}"

    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    
    return output_filename

def open_config_file(config_file):
    '''open_config_file 
        -- opens and returns the config object
        parameters - 
            config_file: string: config.yml
    '''
    with open(config_file, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        pprint(cfg)
    return cfg


def get_log_file_from_config(config_file):
    ''' get_log_file_from_config -- 
        searches for and returns configfile
        parameters - 
        config_file : string : config.yml
    '''
    #TODO: this could be optimized with configparser or simplified another way 
    cfg = open_config_file(config_file)
    if 'env' in cfg:
        if 'logs' in cfg['env']:
            logs_path = cfg['env']['logs']
    try:
        if logs_path == '':
            logger.error('need to have a logs_path in the config file')
            return False
    except Exception as err:
        logger.error(err)
        pass
    
    return logs_path

def get_root_path_from_config(config_file):
    ''' get_log_file_from_config -- 
        searches for and returns configfile
        parameters - 
        config_file : string : config.yml
    '''
    #TODO: this could be optimized with configparser or simplified another way 
    
    cfg = open_config_file(config_file)
    root_path = ''
    if 'env' in cfg:
        if 'root_path' in cfg['env']:
            root_path = cfg['env']['root_path']
    try:
        if root_path == '':
            logger.error('need to have a root_path in the config file')
            return False
    except Exception as err:
        logger.error(err)
        pass
    
    return root_path

def get_backup_list_from_config(config_file):
    ''' get the list of backups and return it '''
    
    cfg = open_config_file(config_file)
    pprint(cfg)
    #input('here')
    if 'backups' in cfg:
        return cfg['backups']
        #pprint(cfg['backups'])
    #input('here')
    #return backups


if __name__ == "__main__":
    config_file = "config.yml"
    backups_to_s3(config_file)