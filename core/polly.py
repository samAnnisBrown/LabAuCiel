from contextlib import closing

from core.connection import *
from core.s3 import *


def polly_talk(testtospeech, filename='labaucielPollyOutput.mp3', bucket=get_config_item('s3_bucket_name')):
    client = connect_boto_client('polly', 'us-west-2')

    # Get response from Polly
    response = client.synthesize_speech(
        OutputFormat='mp3',
        Text=testtospeech,
        VoiceId='Geraint'
    )

    # Stream response to variable
    if 'AudioStream' in response:
        with closing(response['AudioStream']) as stream:
            output = stream.read()

    # Write object to S3
    s3.put_object(bucket, filename, output)

    s3link = 'https://s3-' + get_config_item('default_region') + '.amazonaws.com/' + bucket + "/" + filename

    print(s3link)
    # Returns link to S3 bucket
    return s3link


def polly_voices():
    client = connect_boto_client('polly', 'us-west-2')
    print(client.describe_voices()['Voices'])

