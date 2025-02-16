import boto3
import argparse
import json
import time
from typing import Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError, WaiterError
from datetime import datetime

class DMSJobCreator:
    def __init__(self, region_name: str):
        """
        Initialize the DMS Job Creator with AWS region

        Args:
            region_name (str): AWS region name (e.g., 'us-west-2')
        """
        self.dms_client = boto3.client('dms', region_name=region_name)

    def create_event_subscription(self,
                                subscription_name: str,
                                source_ids: list,
                                event_categories: list = None,
                                source_type: str = 'replication-task',
                                sns_topic_arn: str = "arn:aws:sns:ap-southeast-1:856517911253:dms-task-state",
                                enabled: bool = True,
                                tags: list = None) -> Dict[str, Any]:
        """
        Create a DMS event subscription to monitor task status changes
        
        Args:
            subscription_name (str): Name of the event subscription
            source_ids (list): List of source IDs to monitor
            event_categories (list): List of event categories to monitor
            source_type (str): Type of source (e.g., 'replication-task')
            sns_topic_arn (str): ARN of the SNS topic to notify
            enabled (bool): Whether the subscription should be enabled
            tags (list): List of tags to apply to the subscription
            
        Returns:
            Dict[str, Any]: Response from the create_event_subscription API call
        """
        try:
            params = {
                'SubscriptionName': subscription_name,
                'SnsTopicArn': sns_topic_arn,
                'SourceType': source_type,
                'SourceIds': source_ids,
                'Enabled': enabled
            }

            if event_categories:
                params['EventCategories'] = event_categories
            else:
                # Default to monitoring state changes and failures
                params['EventCategories'] = [
                    'failure',
                    'state change'
                ]

            if tags:
                params['Tags'] = tags

            print("\n=== Creating Event Subscription ===")
            print(json.dumps(params, indent=2))
            response = self.dms_client.create_event_subscription(**params)
            return response

        except ClientError as e:
            print("\n❌ Error creating event subscription:")
            print(json.dumps(str(e), indent=2))
            raise

    def wait_for_instance_available(self, instance_arn: str, timeout: int = 900) -> bool:
        start_time = time.time()
        print("\n=== Waiting for Instance to Become Available ===")
        while (time.time() - start_time) < timeout:
            try:
                response = self.dms_client.describe_replication_instances(
                    Filters=[{'Name': 'replication-instance-arn', 'Values': [instance_arn]}]
                )
                
                if response['ReplicationInstances']:
                    status = response['ReplicationInstances'][0]['ReplicationInstanceStatus']
                    if status == 'available':
                        print(f"✅ Instance is now available")
                        return True
                    elif status == 'failed':
                        print(f"❌ Instance creation failed with status: {status}")
                        raise Exception(f"Instance creation failed with status: {status}")
                    
                    elapsed = round(time.time() - start_time)
                    print(f"Status: {status} | Elapsed Time: {elapsed} seconds")
                
                time.sleep(30)
                
            except ClientError as e:
                print("\n❌ Error checking instance status:")
                print(json.dumps(str(e), indent=2))
                raise
        
        raise TimeoutError(f"Instance did not become available within {timeout} seconds")

    def create_replication_instance(self,
                                  instance_identifier: str,
                                  instance_class: str,
                                  allocated_storage: int,
                                  engine_version: str,
                                  availability_zone: str,
                                  multi_az: bool = False,
                                  publicly_accessible: bool = False,
                                  vpc_security_group_ids: list = None,
                                  subnet_group_identifier: str = None,
                                  tags: list = None) -> str:
        """
        Create a new DMS replication instance

        Args:
            instance_identifier (str): Unique identifier for the replication instance
            instance_class (str): The compute and memory capacity of the instance (e.g., 'dms.t3.micro')
            allocated_storage (int): Storage allocated to the instance in GB
            engine_version (str): The version of the engine
            availability_zone (str): The EC2 Availability Zone where the instance should be located
            multi_az (bool): Specify if the instance is Multi-AZ
            publicly_accessible (bool): Specify if the instance should be publicly accessible
            vpc_security_group_ids (list): List of VPC security group IDs
            subnet_group_identifier (str): The subnet group to place the instance in
            tags (list): List of tags to apply to the instance
            
        Returns:
            str: ARN of the created replication instance
        """
        try:
            params = {
                'ReplicationInstanceIdentifier': instance_identifier,
                'ReplicationInstanceClass': instance_class,
                'AllocatedStorage': allocated_storage,
                'AvailabilityZone': availability_zone,
                'MultiAZ': multi_az,
                'PubliclyAccessible': publicly_accessible,
                'EngineVersion': engine_version
            }
            
            if vpc_security_group_ids:
                params['VpcSecurityGroupIds'] = vpc_security_group_ids
                
            if subnet_group_identifier:
                params['ReplicationSubnetGroupIdentifier'] = subnet_group_identifier
    
            if tags:
                params['Tags'] = tags
            
            print("\n=== Creating Replication Instance ===")
            print(json.dumps(params, indent=2))
            response = self.dms_client.create_replication_instance(**params)
            instance_arn = response['ReplicationInstance']['ReplicationInstanceArn']
            
            # Wait for instance to be available
            if not self.wait_for_instance_available(instance_arn):
                raise Exception("Timeout waiting for replication instance to become available")
            
            return response
            
        except ClientError as e:
            print("\n❌ Error creating replication instance:")
            print(json.dumps(str(e), indent=2))
            raise
        
    def create_replication_task(self,
                              replication_task_identifier: str,
                              source_endpoint_arn: str,
                              target_endpoint_arn: str,
                              replication_instance_arn: str,
                              table_mappings: Dict[str, Any],
                              task_settings: Dict[str, Any] = None,
                              tags: list = None) -> Dict[str, Any]:
        """
        Create a new DMS replication task
        [Previous docstring remains the same]
        """
        try:
            params = {
                'ReplicationTaskIdentifier': replication_task_identifier,
                'SourceEndpointArn': source_endpoint_arn,
                'TargetEndpointArn': target_endpoint_arn,
                'ReplicationInstanceArn': replication_instance_arn,
                'MigrationType': 'full-load',
                'TableMappings': json.dumps(table_mappings)
            }
            
            if task_settings:
                params['ReplicationTaskSettings'] = json.dumps(task_settings)

            if tags:
                params['Tags'] = tags
            
            print("\n=== Creating Replication Task ===")
            print(json.dumps(params, indent=2))
            response = self.dms_client.create_replication_task(**params)
            
            task_arn = response['ReplicationTask']['ReplicationTaskArn']
            
            print(f"\n=== Waiting for Task to be Ready: {replication_task_identifier} ===")
            waiter = self.dms_client.get_waiter('replication_task_ready')
            waiter.wait(
                Filters=[{
                    'Name': 'replication-task-arn',
                    'Values': [task_arn]
                }]
            )
            return response

        except (ClientError, WaiterError) as e:
            print("\n❌ Error creating DMS replication task:")
            print(json.dumps(str(e), indent=2))
            raise

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Create AWS DMS Replication Task')
    
    # Required parameters
    parser.add_argument('--task-identifier', required=True,
                       help='Unique identifier for the replication task')
    parser.add_argument('--source-arn', required=True,
                       help='Source endpoint ARN')
    parser.add_argument('--target-arn', required=True,
                       help='Target endpoint ARN')
    parser.add_argument('--table-mappings', required=True, type=str,
                       help='JSON string containing table mappings')
    
    # Optional parameters
    parser.add_argument('--region', default='ap-southeast-1',
                       help='AWS region (e.g., us-west-2)')
    parser.add_argument('--instance-arn',
                       help='Replication instance ARN (if not provided, a new instance will be created)')
    parser.add_argument('--task-settings', type=str,
                       help='JSON string containing task settings')
    parser.add_argument('--tags', type=str,
                       help='JSON string containing tags')

    # Instance creation parameters
    parser.add_argument('--instance-class', default='dms.t3.micro',
                       help='Instance class (e.g., dms.t3.micro)')
    parser.add_argument('--allocated-storage', type=int, default=50,
                       help='Storage allocated to the instance in GB')
    parser.add_argument('--engine-version', default='3.5.2',
                        help='Engine Version (e.g., 3.5.2)')
    parser.add_argument('--multi-az', action='store_true',
                       help='Enable Multi-AZ deployment')
    parser.add_argument('--publicly-accessible', action='store_true',
                       help='Make instance publicly accessible')
    parser.add_argument('--vpc-security-groups', type=str,
                       help='JSON string array of VPC security group IDs')
    parser.add_argument('--subnet-group',
                       help='Subnet group identifier')
    parser.add_argument('--availability-zone', default='ap-southeast-1a',
                       help='Availability Zone for the instance')
    
    args = parser.parse_args()
    
    if (not args.instance_arn) and (not args.vpc_security_groups):
        parser.error("--vpc-security-groups is required if --instance-arn is not provided")
    
    if (not args.instance_arn) and (not args.subnet_group):
        parser.error("--subnet-group is required if --instance-arn is not provided")
    
    return args

def main():
    """Main execution function"""
    args = parse_arguments()
    
    # Parse JSON strings to objects
    try:
        region = args.region if args.region else "ap-southeast-1"
        task_identifier = f'{args.task_identifier}-{datetime.now().strftime("%Y%m%d")}'
        table_mappings = json.loads(args.table_mappings)
        task_settings = json.loads(args.task_settings) if args.task_settings else None
        tags = json.loads(args.tags) if args.tags else None
        vpc_security_groups = json.loads(args.vpc_security_groups) if args.vpc_security_groups else None
    except json.JSONDecodeError as e:
        print("\n❌ Error parsing JSON input:")
        print(json.dumps(str(e), indent=2))
        raise
    
    # Create DMS job
    dms_creator = DMSJobCreator(region)
    try:
        instance_arn = args.instance_arn
        
        # Create instance if not provided
        if not instance_arn:
            instance_params = {
                'instance_identifier': f"{task_identifier}-instance",
                'instance_class': args.instance_class,
                'allocated_storage': args.allocated_storage,
                'multi_az': args.multi_az,
                'publicly_accessible': args.publicly_accessible,
                'vpc_security_group_ids': vpc_security_groups,
                'availability_zone': args.availability_zone,
                'subnet_group_identifier': args.subnet_group,
                'engine_version': args.engine_version,
                'tags': tags
            }
            instance_response = dms_creator.create_replication_instance(**instance_params)
            instance_arn = instance_response['ReplicationInstance']['ReplicationInstanceArn']
            print("\n✅ Successfully created replication instance:")
            print(json.dumps(instance_arn, indent=2))

        # Create the replication task
        task_response = dms_creator.create_replication_task(
            replication_task_identifier=task_identifier,
            source_endpoint_arn=args.source_arn,
            target_endpoint_arn=args.target_arn,
            replication_instance_arn=instance_arn,
            table_mappings=table_mappings,
            task_settings=task_settings,
            tags=tags
        )
        print("\n✅ Successfully created DMS replication task:")
        print(json.dumps(task_response, indent=2))
        
        # Start the replication task
        print("\n=== Starting Replication Task ===")
        start_response = dms_creator.dms_client.start_replication_task(
            ReplicationTaskArn=task_response['ReplicationTask']['ReplicationTaskArn'],
            StartReplicationTaskType='start-replication'
        )
        print("\n✅ Successfully started replication task:")
        print(json.dumps(start_response, indent=2))
        
        # Create the event subscription
        subscription_response = dms_creator.create_event_subscription(
            subscription_name=f"{task_identifier}-subscription",
            source_ids=[task_identifier]
        )
        print("\n✅ Successfully created event subscription:")
        print(json.dumps(subscription_response, indent=2))
        
    except Exception as e:
        print("\n❌ Failed to create DMS replication task:")
        print(json.dumps(str(e), indent=2))
        raise

if __name__ == "__main__":
    main()