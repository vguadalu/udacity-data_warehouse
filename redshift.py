import boto3
import json
import configparser
import pandas as pd
import argparse
import sys

def create_iam_client(KEY, SECRET):
    """
    Creates the IAM client to be used to connect to the redshift cluster
    
    Parameters
    ----------
    KEY: str
    SECRET: str
    """
    iam = boto3.client('iam',
                       region_name='us-east-1',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    return iam

    
def create_iam_role(iam):
    """
    Creates the IAM role to be used with the IAM client.
    
    Parameters
    ----------
    KEY: str
    SECRET: str
    """
    try:
        iam.create_role(Path='/',
                       RoleName='sparkify-role',
                       Description='BOTO3 iam role for Sparkify',
                       AssumeRolePolicyDocument=json.dumps(
                           {'Statement': [{'Action': 'sts:AssumeRole',
                                         'Effect': 'Allow',
                                         'Principal':{'Service': 'redshift.amazonaws.com'}}],
                                        'Version': '2012-10-17'}))
    except Exception as e:
        print(e)
        
        
def attach_policy_to_iam(iam):
    """
    Attaches desired policies to the IAM role
    
    Parameters:
    -----------
    iam: IAM Client
    """
    iam.attach_role_policy(RoleName='sparkify-role',
                          PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    
    
def get_ARN(iam):
    """
    Get the ARN from the IAM Role
    
    Parameters
    ----------
    iam: IAM client
    """
    ARN = iam.get_role(RoleName='sparkify-role')['Role']['Arn']
    return ARN


def write_in_config(ARN, Endpoint):
    """
    Write updated ARN and Endpoint to the config that will be used by etl.py and create_tables.py
    
    Parameters
    ----------
    ARN: str
    Endpoint: str
    """
    cfg_lines = open('dwh.cfg', 'r').readlines()
    
    with open('dwh.cfg', 'w') as cfg:
        for line in cfg_lines:
            if 'ARN' in line:
                cfg.write(line.replace(line.split("=")[-1], f'{ARN}\n'))
            elif 'HOST' in line:
                cfg.write(line.replace(line.split("=")[-1], f'{Endpoint}\n'))
            else:
                cfg.write(line)
            

def create_redshift_client(KEY, SECRET):
    """
    Creates the redshift client
    
    Parameters
    ----------
    KEY: str
    SECRET: str
    """
    redshift = boto3.client('redshift',
                         region_name='us-east-1',
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)
    return redshift


def create_redshift_cluster(redshift, DB_NAME, DB_USER, DB_PASSWORD, ARN):
    """
    Creates a redshift cluster using the previoulsy generated redshift client
    
    Parameters
    ----------
    redshift: client
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD:str
    ARN: str
    """
    try:
        redshift.create_cluster(ClusterType='multi-node',
                                NodeType='dc2.large',
                                NumberOfNodes=4,
                                DBName=DB_NAME,
                                ClusterIdentifier='sparkify-cluster',
                                MasterUsername=DB_USER,
                                MasterUserPassword=DB_PASSWORD,
                                IamRoles=[ARN],
                                VpcSecurityGroupIds=['sg-0b9d647cb7001e060'])
    except Exception as e:
        print(e)


def prettyRedshiftProps(props):
    """
    Displays information in a nice table design
    This function was taken from the Udacity Cloud Data Warehouse Lesson 3 Examples
    
    Parameters
    -----------
    props: redshift cluster's props
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def check_status_of_cluster():
    """
    Grabs information about the cluster
    
    Returns
    -------
    Status of cluster
    """
    myClusterProps = redshift.describe_clusters(ClusterIdentifier='sparkify-cluster')['Clusters'][0]
    print(prettyRedshiftProps(myClusterProps))
    return redshift.describe_clusters(ClusterIdentifier='sparkify-cluster')['Clusters'][0]["ClusterStatus"]


def get_endpoint(redshift):
    """
    Grabs the endpoint for the redshift cluster
    
    Parameters
    ----------
    redshift: redshift client
    
    Returns
    -------
    endpoint: endpoint from cluster
    """
    endpoint = redshift.describe_clusters(ClusterIdentifier='sparkify-cluster')['Clusters'][0]["Endpoint"]["Address"]
    return endpoint 


def cleanup_resources(redshift, iam):
    """
    Deletes redshift cluster and iam role
    
    Parameters
    ----------
    redshift: redshift client
    iam: iam client
    """
    redshift.delete_cluster( ClusterIdentifier='sparkify-cluster',  SkipFinalClusterSnapshot=True)
    iam.detach_role_policy(RoleName='sparkify-role',                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName='sparkify-role')
    
    
if __name__ == '__main__':
    """
    Reads the config and assigns values to desired variables.
    Based on the purpose argument passed, it will either create the redshift cluster and iam role, check the status of the redshift cluster or delete the redshift cluster and iam role.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('purpose', help='What the purpose of running the script is. \n 
                        'Options: \n create : creates iam role and redshift cluster \n 
                        'check: returns the status of the redshift cluster \n 
                        'delete: detach iam role and redshift cluster ')
    args, unknown = parser.parse_known_args()
    
    if args.purpose not in ['clean', 'check', 'create']:
        print(f'{args.purpose} is not a valid purpose type. Please use "clean", "check" or "create"')
        sys.exit(1)
        
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    
    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    
    iam = create_iam_client(KEY, SECRET)
    redshift = create_redshift_client(KEY, SECRET)
    
    if args.purpose == 'create':
        # iam role
        create_iam_role(iam)
        attach_policy_to_iam(iam)
        ARN = get_ARN(iam)
        # redshift
        create_redshift_cluster(redshift, DB_NAME, DB_USER, DB_PASSWORD, ARN)
        
    if args.purpose == 'check':
        cluster_status = check_status_of_cluster()
        if cluster_status == 'available':
            ARN = get_ARN(iam)
            Endpoint = get_endpoint(redshift)
            #update config
            write_in_config(ARN, Endpoint)
        
    if args.purpose == 'clean':
        cleanup_resources(redshift,iam)
