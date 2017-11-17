from core.polly import *


class rekog():

    @staticmethod
    def detectObject(image, voice):

        client = connect_boto_client('rekognition', 'us-west-2')

        response = client.detect_labels(
            Image={
                'Bytes': image,
            },
            MaxLabels=1,
        )

        #s3.putObject(get_config_item('s3_bucket_name'), 'rekognition/latest.jpeg', image)

        foundobject = True

        # Check if something is found
        try:
            response['Labels'][0]['Name']
        except:
            foundobject = False

        # If found, do this
        if foundobject:
            # If the object is a person
            if response['Labels'][0]['Name'] == 'People' or response['Labels'][0]['Name'] == 'Human':

                # Check to see if it's a celebrity
                celeb = client.recognize_celebrities(
                    Image={
                        'Bytes': image,
                    }
                )

                celebexist = True

                try:
                    celeb['CelebrityFaces'][0]['Name']
                except:
                    celebexist = False

                # If a celebrity is found
                if celebexist:

                    celebname = celeb['CelebrityFaces'][0]['Name']
                    celebconfidence = str(round(celeb['CelebrityFaces'][0]['Face']['Confidence'], 2))

                    text = "Well, that's definitely a famous person!  I\'m " + celebconfidence + " percent confident that I\'m looking at " + celebname + "."
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
                    emotion = str(person['FaceDetails'][0]['Emotions'][0]['Type']).lower()
                    agelow = str(person['FaceDetails'][0]['AgeRange']['Low'])
                    agehigh = str(person['FaceDetails'][0]['AgeRange']['High'])

                    if smiling:
                        text = 'I\'m pretty sure this person is ' + gender + ', and it looks like they\'re ' + emotion + " and smiling.\n They're likely between " + agelow + " and " + agehigh + " years old."
                    else:
                        text = 'I\'m pretty sure this person is ' + gender + ', and it looks like they\'re ' + emotion + " and aren't smiling.\n They're likely between " + agelow + " and " + agehigh + " years old."

                    print(text)

            # If it's not a person, return object details
            else:

                object = str(response['Labels'][0]['Name']).lower()
                confidence = str(round(response['Labels'][0]['Confidence'], 2))

                if object[0] in ('a', 'e', 'i', 'o', 'u'):
                    text = "I\'m " + confidence + " percent sure I\'m looking at an " + object + "."
                else:
                    text = "I\'m " + confidence + " percent sure I\'m looking at a " + object + "."
                print(text)

        # Nothing found at all
        else:
            text = "I don\'t know what the hell I\'m looking at!"
            print(text)

        return polly.toS3(text, voice), text




