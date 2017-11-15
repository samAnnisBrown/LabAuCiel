from core.s3 import *


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

        print("I\'m " + str(response['Labels'][0]['Confidence']) + " confident that I\'m looking at. " + str(response['Labels'][0]['Name']))

        #s3.putObject(get_config_item('s3_bucket_name'), 'rekognition/latest.jpeg', image)


