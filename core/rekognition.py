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

        # If the response is a person
        if response['Labels'][0]['Name'] == 'People' or response['Labels'][0]['Name'] == 'Human':

            # Check to see if it's a celebrity
            celeb = client.recognize_celebrities(
                Image={
                    'Bytes': image,
                }
            )

            print(celeb)
            celebexist = True

            try:
                celeb['CelebrityFaces'][0]['Name']
            except:
                celebexist = False

            # If a celebrity is found
            if celebexist:

                celebname = celeb['CelebrityFaces'][0]['Name']
                celebconfidence = str(round(celeb['CelebrityFaces'][0]['Face']['Confidence'], 1))

                text = "I\'m " + celebconfidence + " percent confident that I\'m looking at " + celebname
                print(text)

            # If not a celebrity, get details on the face
            else:

                person = client.detect_faces(
                    Image={
                        'Bytes': image,
                    },
                    Attributes=[
                        'ALL',
                    ]
                )


                gender = person['FaceDetails'][0]['Gender']['Value']
                smiling = person['FaceDetails'][0]['Smile']['Value']
                emotion = person['FaceDetails'][0]['Emotions'][0]['Type']

                if smiling:
                    text = 'This person is ' + gender + ', and it looks like they\'re ' + emotion + " and smiling"
                else:
                    text = 'This person is ' + gender + ', and it looks like they\'re ' + emotion + ".  They're not smiling"

                print(text)
                print(person)
        # If it's not a person, return object details
        else:
            text = "I\'m " + str(round(response['Labels'][0]['Confidence'], 1)) + " percent confident that I\'m looking at a " + str(response['Labels'][0]['Name'])
            print(text)

        return polly.toS3(text, 'Geraint')




