import boto3
import cfnresponse

def handler(event, context):
    the_event = event['RequestType']
    print("The event is: ", str(the_event))
    response_data = {}
    s_3 = boto3.client('s3')
    the_bucket = event['ResourceProperties']['the_bucket']
    dirs_to_create = event['ResourceProperties']['dirs_to_create']
    try:
        if the_event in ('Create', 'Update'):
            print("Requested folders: ", str(dirs_to_create))
            for dir_name in dirs_to_create:
                print("Creating: ", str(dir_name))
                s_3.put_object(Bucket=the_bucket, Key=(dir_name + '/'))
        elif the_event == 'Delete':
            print("Deleting S3 content...")
            b_operator = boto3.resource('s3')
            b_operator.Bucket(str(the_bucket)).objects.all().delete()
        print("Operation successful!")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
    except Exception as e:
        print("Operation failed...")
        print(str(e))
        response_data['Data'] = str(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data)