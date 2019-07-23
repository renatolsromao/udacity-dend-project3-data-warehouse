import configparser
import json
import time

import boto3 as boto3


def create_role_and_get_name(iam):
    role_name = 'dend-project-role'

    try:
        iam_role = iam.get_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:

        iam.create_role(
            Path='/',
            RoleName=role_name,
            Description='Allows Redshift to call AWS Services.',
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'
            })
        )

        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )

        iam_role = iam.get_role(RoleName=role_name)

    return iam_role['Role']['Arn']


def create_redshift_cluster(redshift, db_name=None, db_user=None, db_password=None, iam_role_arn=None,
                            cluster_type='multi-node', node_type='dc2.large', nodes=2, identifier='dend-cluster'):
    redshift.create_cluster(
        # hardware
        ClusterType=cluster_type,
        NodeType=node_type,
        NumberOfNodes=int(nodes),

        # identifiers & credentials
        DBName=db_name,
        ClusterIdentifier=identifier,
        MasterUsername=db_user,
        MasterUserPassword=db_password,

        # role
        IamRoles=[iam_role_arn]
    )

    while get_cluster_status(identifier, redshift) == 'creating':
        print('Creating Cluster..')
        time.sleep(30)

    if get_cluster_status(identifier, redshift) != 'available':
        raise ValueError('Error while creating Redshift Cluster.')


def get_cluster_properties(identifier, redshift):
    return redshift.describe_clusters(ClusterIdentifier=identifier)['Clusters'][0]


def get_cluster_status(identifier, redshift):
    cluster_properties = get_cluster_properties(identifier, redshift)
    return cluster_properties['ClusterStatus']


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    boto3.setup_default_session(region_name=config.get('AWS', 'REGION'),
                                aws_access_key_id=config.get('AWS', 'KEY'),
                                aws_secret_access_key=config.get('AWS', 'SECRET'))

    iam_client = boto3.client('iam')
    redshift_client = boto3.client('redshift')

    iam_role_arn = create_role_and_get_name(iam_client)
    create_redshift_cluster(
        redshift_client, identifier=config.get('CLUSTERCONFIG', 'IDENTIFIER'),
        nodes=config.get('CLUSTERCONFIG', 'NODES'), cluster_type=config.get('CLUSTERCONFIG', 'TYPE'),
        node_type=config.get('CLUSTERCONFIG', 'NODE_TYPE'),
        db_name=config.get('CLUSTER', 'DB_NAME'), db_user=config.get('CLUSTER', 'DB_USER'),
        db_password=config.get('CLUSTER', 'DB_PASSWORD'), iam_role_arn=iam_role_arn)

    cluster_properties = get_cluster_properties(config.get('CLUSTERCONFIG', 'IDENTIFIER'), redshift_client)
    print('CLUSTER ENDPOINT: {}\nCLUSTER ARN: {}'
          .format(cluster_properties['Endpoint']['Address'], cluster_properties['IamRoles'][0]['IamRoleArn']))


if __name__ == '__main__':
    main()
