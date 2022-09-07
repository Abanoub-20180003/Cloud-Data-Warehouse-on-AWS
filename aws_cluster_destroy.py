import boto3
import configparser

def config_parser():
    """
    Set the AWS Config parameters with value from dwh.cfg config file
    """

    global KEY, SECRET, DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME, ARN

    print("Parsing the configuration file...\n")

    config = configparser.ConfigParser()
    with open('dwh2.cfg') as configfile:
        config.read_file(configfile)

        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')


        DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
        DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        ARN = config.get("DWH", "arn")
        
        
def aws_client(service, region):
    """
    Creates an AWS client (specified by the argument) in region (specified by argument)
    :param service: The service to be created
    :param region: The region where service has to be created
    :return client: The client for AWS service
    """

    global KEY, SECRET
    client = boto3.client(service, aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=region)

    return client
        
        
def redshift_cluster_status(redshift):
    """
    Retrieves the Redshift cluster status
    :param redshift: Boto3 client for Redshift
    :return cluster_status: The cluster status
    """
    
    global DWH_CLUSTER_IDENTIFIER
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus'].lower()
    print("Cluster Status", cluster_status)
    return cluster_status        
        

def destroy_redshift_cluster(redshift):
    """
    Destroy the Redshift cluster
    :param redshift: Boto3 client for Redshift
    """
    global DWH_CLUSTER_IDENTIFIER
    print("destroying redshift cluster... ")
    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    except Exception as e: 
        return 0
    
    
def destroy_iam_role(iam):
      
    global  DWH_IAM_ROLE_NAME, ARN
    print("destroying iam role... ")   
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
       
        
        
def main():
    config_parser()    
        
    # create resources cli
    redshift = aws_client('redshift', "us-west-2")
    iam = aws_client('iam', "us-west-2")

    # destroy redshift cluster 
    if redshift_cluster_status(redshift) == 'available':
        print('Cluster status: available')
        destroy_redshift_cluster(redshift)
        print('Cluster status: ', redshift_cluster_status(redshift))
    else:
        print("Cluster not available.")
    
    
    # destroy iam role 
    destroy_iam_role(iam)
    
    print('destroying done.')

if __name__ == '__main__':
    main()
