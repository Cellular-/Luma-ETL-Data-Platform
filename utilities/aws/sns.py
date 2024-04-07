import boto3
from config.config import get_config

class AWSSNSService:
    def __init__(self, config):
        self._config = config
        self.topic_arn = config.get('aws', 'sns_etl_failure_topic')
        self.aws_access_key_id = config.get('aws', 'access_key_id')
        self.aws_secret_access_key = config.get('aws', 'secret_access_key')
        self.region_name = config.get('aws', 'region')

        # Create an SNS client
        self.sns_client = boto3.client('sns', region_name=self.region_name)

    def publish_message(self, subject=None, message=None, topic_arn=None):
        response = self.sns_client.publish(
            TopicArn=topic_arn or self.topic_arn,
            Message=message,
            Subject=subject
        )

if __name__ == '__main__':
    aws_sns = AWSSNSService(
        config=get_config()
    )
    new_message = {
        'message': 'Email test',
        'subject': 'Email testing from Enterprise Reporting ETL'
    }

    try:
        aws_sns.publish_message(
            **new_message
        )

        print(f'''Sent message to {aws_sns.topic_arn}
        {new_message["subject"]} {new_message["message"]}'''.rstrip())
    except Exception as e:
        print(f'Could not send email: {e}')