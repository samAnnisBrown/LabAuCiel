from core.s3 import *
from core.polly import *


class rekog():

    @staticmethod
    def detectObject(image):

        client = connect_boto_client('rekognition', 'us-west-2')

        response = client.detect_labels(
            Image={
                'Bytes': image,
            },
            MaxLabels=1,
        )

        #s3.putObject(get_config_item('s3_bucket_name'), 'rekognition/latest.jpeg', image)

        if response['Labels'][0]['Name'] == 'People':

            response = client.detect_faces(
                Image={
                    'Bytes': image,
                },
                Attributes=[
                    'ALL',
                ]
            )


            gender = response['FaceDetails'][0]['Gender']['Value']
            smiling = response['FaceDetails'][0]['Smile']['Value']
            emotion = response['FaceDetails'][0]['Emotions'][0]['Type']

            if smiling:
                text = 'This person is ' + gender + ', and it looks like they\'re ' + emotion + " and smiling"
            else:
                text = 'This person is ' + gender + ', and it looks like they\'re ' + emotion + ".  They're not smiling"

            print(text)
            print(response)
        else:
            text = "I\'m " + str(round(response['Labels'][0]['Confidence'], 1)) + " percent confident that I\'m looking at a " + str(response['Labels'][0]['Name'])
            print(text)

        return polly.toS3(text, 'Geraint')




