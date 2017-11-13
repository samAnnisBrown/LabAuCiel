from contextlib import closing

from core.connection import *
from core.s3 import *


def polly_talk(textin):
    client = connect_boto_client('polly', 'us-west-2')
    print(textin)

    response = client.synthesize_speech(
        OutputFormat='mp3',
        Text=textin,
        VoiceId='Geraint'
    )

    if 'AudioStream' in response:
        with closing(response['AudioStream']) as stream:
            output = "polly-boto.mp3"
    print(response['AudioStream'])
    s3.put_object('labauciel-pollyout', 'doesitwork.mp3', response['AudioStream'])

    audiofile = 'placeholder'
    return audiofile


def polly_voices():
    client = connect_boto_client('polly', 'us-west-2')
    print(client.describe_voices()['Voices'])

