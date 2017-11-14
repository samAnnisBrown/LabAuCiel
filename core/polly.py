from contextlib import closing
from core.s3 import *


class polly():

    @staticmethod
    def toS3(testtospeech, key='labaucielPollyOutput.mp3', bucket=get_config_item('s3_bucket_name'), region='us-west-2'):
        client = connect_boto_client('polly', region)

        # Get response from Polly
        response = client.synthesize_speech(
            OutputFormat='mp3',
            Text=testtospeech,
            VoiceId='Geraint'
        )

        # Stream response to S3
        if 'AudioStream' in response:
            with closing(response['AudioStream']) as stream:
                output = stream.read()
                s3.putObject(bucket, key, output, region)

        s3link = 'https://s3-' + get_config_item('default_region') + '.amazonaws.com/' + bucket + "/" + key

        print(s3link)

        # Returns link to S3 bucket
        return s3link

    @staticmethod
    def list_polly_voices():
        client = connect_boto_client('polly', 'us-west-2')
        print(client.describe_voices()['Voices'])

