from contextlib import closing

from core.s3 import *


class polly():

    @staticmethod
    def toS3(testtospeech, voice, key='polly/labaucielPollyOutput.mp3', bucket=get_config_item('s3_bucket_name'), region='us-west-2'):
        client = connect_boto_client('polly', region)

        # Get response from Polly
        response = client.synthesize_speech(
            OutputFormat='mp3',
            Text=testtospeech,
            VoiceId=voice
        )

        # Stream response to S3
        if 'AudioStream' in response:
            with closing(response['AudioStream']) as stream:
                output = stream.read()
                s3.putObject(bucket, key, output, region)

        url = s3.presignedUrl(bucket, key)

        return url

    @staticmethod
    def listVoices():
        client = connect_boto_client('polly', 'us-west-2')

        voices = client.describe_voices()['Voices']
        # Code for jsonifying - for posterity
        #output = []

        #for voice in voices:
        #    item = {'Name': voice['Name']}, {'Language': voice['LanguageName']}, {'Gender': voice['Gender']}
        #    output.append(item)

        return voices

