def run_glue_job(job_name, client_glue):
    ''' 
    Function to run an AWS Glue job

    Parameters:
    - job_name: str: Name of the Glue job to run
    - aws_credentials_block: AwsCredentials: Prefect block with the AWS credentials

    Returns:
    - str: Status of the job run
    '''
    import time

    # client = aws_credentials_block.get_boto3_session().client("glue")

    response = client_glue.start_job_run(JobName=job_name)

    print(f"Job run started with runId: {response['JobRunId']}")

    # Monitor the job run
    while True:
        status = client_glue.get_job_run(JobName=job_name, RunId=response['JobRunId'])['JobRun']['JobRunState']
        print(f"Current job run status: {status}")

        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break

        print("Waiting for job run to complete...")
        time.sleep(10)  # wait for 10 seconds before checking the status again

    print(f"Final job run status: {status}")
    
    print('Waiting for 30 seconds to avoid exceeding the maximun number of concurrent runs for the AWS Glue job...')
    time.sleep(30)
    return status


def run_crawler(crawler_name, client_glue):
    '''
    Function to run an AWS Glue crawler

    Parameters:
    - crawler_name: str: Name of the Glue crawler to run
    - aws_credentials_block: AwsCredentials: Prefect block with the AWS credentials

    Returns:
    - status: str: Status of the crawler run
    '''
    import time
    # client = aws_credentials_block.get_boto3_session().client("glue")

    response = client_glue.start_crawler(
            Name=crawler_name
        )

    print(f"Crawler started: {response}")

    # Wait for the crawler to complete
    while True:
        response = client_glue.get_crawler(
            Name=crawler_name
        )

        status = response['Crawler']['State']
        print(f"Crawler status: {status}")

        if status == 'READY':
            print("Crawler has completed.")
            break

        print("Waiting for crawler to complete...")
        time.sleep(15)  # wait for 10 seconds before checking the status again 
    return status


def delete_results(bucket_name, folder_name, s3_client):
    '''
    Function to delete the results of the execution of the flow

    Parameters:
    - bucket_name: str: Name of the bucket where the results are stored
    - folder_name: str: Name of the folder where the results are stored
    - aws_credentials_block: AwsCredentials: Prefect block with the AWS credentials

    Returns:
    - None
    '''

    # client = aws_credentials_block.get_boto3_session().client("s3")
    objects_to_delete = s3_client.list_objects(Bucket=bucket_name, Prefix=folder_name)

    print('Deleting results...')

    delete_keys = {'Objects': []}
    for key in objects_to_delete['Contents']:
        delete_keys['Objects'].append({'Key': key['Key']})

    if delete_keys['Objects']:
        s3_client.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    print('Results deleted successfully')
    return None

