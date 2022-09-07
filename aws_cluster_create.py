import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import boto3
import json 
import time 

def create_iam(config, iam):
    """
    Description: create iam role to able redshift to access s3 
    Parameters: configuration file and iam client
    Return: iam role arn 
    """
     # iam role config to able redshift access s3
    DWH_IAM_ROLE_NAME   = config.get("DWH", "dwh_iam_role_name")
    print('DWH_IAM_ROLE_NAME', DWH_IAM_ROLE_NAME)
    
    # create iam if exsits 
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    
   
    # create iam role 
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'}
            )       
        )    
    except Exception as e:
        print(e)
        
    # add Policy to the iam role
    print('1.2 Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']
    # get iam arn
    roleArn = iam.get_role(RoleName= DWH_IAM_ROLE_NAME)['Role']['Arn']
    return roleArn

def create_redshift_cluster(config, redshift, roleArn):
    """
    Description: create the redshift cluster 
    Parameters: configurations file object, redshift client, iamrole 
    Return: cluster status (success or false)
    """
    print("creating Cluster ...")
    # get harware configuration 
    DWH_CLUSTER_TYPE       = config.get("DWH","dwh_cluster_type")
    DWH_NUM_NODES          = config.get("DWH","dwh_num_nodes")
    DWH_NODE_TYPE          = config.get("DWH","dwh_node_type")
    
    print("Cluster Info:")
    print("DWH_CLUSTER_TYPE:", DWH_CLUSTER_TYPE)
    print("DWH_NUM_NODES:", DWH_NUM_NODES)
    print("DWH_NODE_TYPE:",  DWH_NODE_TYPE)
    
    # cluster configuration
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","dwh_cluster_identifier")
    DWH_DB = config.get("DWH","db_name")
    DWH_DB_USER = config.get("DWH","db_user")
    DWH_DB_PASSWORD = config.get("DWH","db_password")
    print("DWH_CLUSTER_IDENTIFIER:", DWH_CLUSTER_IDENTIFIER)
    print("DWH_DB:", DWH_DB)
    print("DWH_DB_USER:",  DWH_DB_USER)
    print("DWH_DB_PASSWORD:",  DWH_DB_PASSWORD)
    # create redshift claster 
    try:
        response = redshift.create_cluster(        
            # TODO: add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # TODO: add parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # TODO: add parameter for role (to allow s3 access)
            IamRoles=[roleArn]  
        )
        
        print("inside status: ", response['ResponseMetadata']['HTTPStatusCode'])
        return response['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)
        return False 
     



def open_cluster_port(ec2, cluster_client, config):
    """
    Description: Opens an incoming TCP port to access Redshift cluster endpoint on VPC security group
    Parameters: ec2 resourse, redshift cluster client, config file 
    Return: non 
    """
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    # getting cluster pros 
    myClusterProps = cluster_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    # Open an incoming TCP port to access the cluster ednpoint
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,  
            CidrIp='0.0.0.0/0',  
            IpProtocol='TCP',  
            FromPort=int(5439),
            ToPort=int(5439)
        )
    except Exception as e:
        return 0 

    
def cluster_status(cluster_client, config):
    """
    Description: Retrieves the Redshift cluster status
    Parameters: cluster client, config file object
    Return: The cluster status
    """
    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'dwh_cluster_identifier')
    cluster_props = cluster_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus'].lower()
    return cluster_status


def update_config(redshift, config):
    """
    Description: Write the cluster endpoint and IAM ARN string to the dwh.cfg configuration file
    Parameters: cluster client, config file object
    Return: non
    """
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    print("Writing the cluster endpoint address and IAM Role ARN to the config file...\n")
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    config.set("DWH", "HOST", cluster_props['Endpoint']['Address'])
    config.set("DWH", "ARN", cluster_props['IamRoles'][0]['IamRoleArn'])

    with open('dwh2.cfg', 'w+') as configfile:
        config.write(configfile)



    
def main():
    # first ########################################################################
    # prepare cluster coonfigurations, resourses and clinets 
    # read configuration file
    config = configparser.ConfigParser()
    config.read('dwh2.cfg')
    print('# Sections',config.sections())
    
    # Admin User Key and Secret
    KEY          = config.get('AWS', 'KEY')
    SECRET       = config.get('AWS', 'SECRET')
    
    # create redshift client 
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    # create iam client
    iam = boto3.client('iam',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    # create ec2 resourse to able access redshift cluster 
    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
    # end first ####################################################################
    
    # second ########################################################################
    # create iam role and redshift cluster 
    # create iam role 
    roleArn = create_iam(config, iam)
    print("Iam Role Created, ARN:", roleArn)

    # create redshift cluster 
    mycluster_status = create_redshift_cluster(config, redshift, roleArn)       
    print("Cluster Status Code", cluster_status)
    
    # print cluster state
    # prettyRedshiftProps(myClusterProps)
    
    
    #check for cluster status
    if mycluster_status:
        print("Creating Redshift Cluster...")

        while True:
            print("Checking if the cluster is created...")

            if cluster_status(redshift, config) == 'available':
                update_config(redshift, config)
                open_cluster_port(ec2, redshift, config)
                break
            else:
                print("Cluster is still being created. Please wait.")

            time.sleep(30)
        print("Cluster creation successful.\n")    
    # end second ####################################################################
    
 


if __name__ == "__main__":
    main()