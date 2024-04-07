import boto3

def activate_trigger(trigger_name: str):
    """
    Activates an AWS Glue trigger.

    trigger_name -- name of trigger
    """
    session = boto3.Session()

    client = session.client('glue')
    return client.start_trigger(Name=trigger_name)

