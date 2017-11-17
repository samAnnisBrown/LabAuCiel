from core.polly import *


class rekog():

    @staticmethod
    def detectObject(image, voice):

        client = connect_boto_client('rekognition', 'us-west-2')

        response = client.detect_labels(
            Image={
                'Bytes': image,
            },
            MaxLabels=4,
        )

        # If found, do this
        try:
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

                    text = "This looks like a celebrity!  I\'m " + celebconfidence + " percent confident that I\'m looking at " + celebname + "."
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
                        text = 'I\'m pretty sure this person is ' + gender + ', and it looks like they are ' + emotion + " and smiling. I think they are between " + agelow + " and " + agehigh + " years old."
                    else:
                        text = 'I\'m pretty sure this person is ' + gender + ', and it looks like they are ' + emotion + " and are not smiling. I think they are between " + agelow + " and " + agehigh + " years old."

                    print(text)

            # If it's not a person, return object details
            else:
                print(len(response['Labels']))
                print(response)
                object = str(response['Labels'][0]['Name']).lower()
                confidence = str(round(response['Labels'][0]['Confidence'], 2))

                numberofobjects = len(response['Labels'])

                if numberofobjects > 1:
                    if object[0] in ('a', 'e', 'i', 'o', 'u'):
                        text = "Hmm... I am " + confidence + " percent sure there is an " + object
                    else:
                        text = "Hmm... I am " + confidence + " percent sure there is a " + object

                    for i in range(1, numberofobjects):
                        object = str(response['Labels'][i]['Name']).lower()
                        confidence = str(round(response['Labels'][i]['Confidence'], 2))
                        if object[0] in ('a', 'e', 'i', 'o', 'u'):
                            text += "; " + confidence + " percent sure there is an " + object
                        else:
                            text += "; " + confidence + " percent sure there is a " + object

                else:
                    if object[0] in ('a', 'e', 'i', 'o', 'u'):
                        text = "I\'m " + confidence + " percent sure I\'m looking at an " + object + "."
                    else:
                        text = "I\'m " + confidence + " percent sure I\'m looking at a " + object + "."
                print(text)

        # Nothing found at all
        except:
            text = "I don\'t know what the hell I\'m looking at!"
            print(text)

        return polly.toS3(text, voice), text




