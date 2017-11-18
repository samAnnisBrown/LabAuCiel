from core.polly import *


class rekog():

    @staticmethod
    def returnPollyUrl(image, voice):
        client = connect_boto_client('rekognition', 'us-west-2')

        objectResponse = client.detect_labels(
            Image={
                'Bytes': image,
            },
            MaxLabels=4,
        )
        print(objectResponse)

        # Let's try to detect what's in the response.
        try:
            # First, see if the object is a person
            if objectResponse['Labels'][0]['Name'] == 'People' or objectResponse['Labels'][0]['Name'] == 'Human':

                # Second, check to see if it's a celebrity
                celebResponse = client.recognize_celebrities(
                    Image={
                        'Bytes': image,
                    }
                )
                print(celebResponse)

                try:
                    celebResponse['CelebrityFaces'][0]['Name']
                    celebexist = True
                except:
                    celebexist = False

                # If a celebrity is found
                if celebexist:
                    text = rekog.detectCelebrity(celebResponse)

                # If not a celebrity, get details on the face
                else:
                    personResponse = client.detect_faces(
                        Image={
                            'Bytes': image,
                        },
                        Attributes=[
                            'ALL',
                        ]
                    )
                    print(personResponse)

                    # Third, it's not a celebrity, so check to see if we can detect details about the face.
                    try:
                        personResponse['FaceDetails'][0]['AgeRange']
                        faceExists = True
                    except:
                        faceExists = False

                    # Forth, if no face details are found, revert back to object definition, otherwise, tell us about the face.
                    if faceExists:
                        text = rekog.detectFace(personResponse)
                    else:
                        text = rekog.detectObject(objectResponse)

            # If it's not a person, return object details immediately
            else:
                text = rekog.detectObject(objectResponse)

        # Either nothing was found, or something went wrong.
        except:
            text = "I don\'t know what the hell I\'m looking at!"
            print(text)

        return polly.toS3(text, voice), text

    @staticmethod
    def detectObject(rekogInput):
        print(rekogInput)
        object = str(rekogInput['Labels'][0]['Name']).lower()
        confidence = str(round(rekogInput['Labels'][0]['Confidence'], 2))

        if len(rekogInput['Labels']) > 1:
            text = "I am " + confidence + " percent sure there " + rekog.conjugateAndArticle(object) + " " + object

            for i in range(1, len(rekogInput['Labels'])):
                object = str(rekogInput['Labels'][i]['Name']).lower()
                confidence = str(round(rekogInput['Labels'][i]['Confidence'], 2))

                if i is len(rekogInput['Labels']) - 1:
                    text += ", and " + confidence + " percent sure there " + rekog.conjugateAndArticle(object) + " " + object + "."
                else:
                    text += ", " + confidence + " percent sure there " + rekog.conjugateAndArticle(object) + " " + object

        else:
            text = "I\'m " + confidence + " percent sure what I\'m looking at " + rekog.conjugateAndArticle(object) + " " + object + "."
        print(text)

        return text

    @staticmethod
    def detectCelebrity(rekogInput):
        celebname = rekogInput['CelebrityFaces'][0]['Name']
        celebconfidence = str(round(rekogInput['CelebrityFaces'][0]['Face']['Confidence'], 2))

        text = "This looks like a celebrity!  I\'m " + celebconfidence + " percent confident that this is " + celebname + "."
        print(text)
        return text

    @staticmethod
    def detectFace(rekogInput):
        # Return boolean
        smiling = rekogInput['FaceDetails'][0]['Smile']['Value']
        eyeglasses = rekogInput['FaceDetails'][0]['Eyeglasses']['Value']
        sunglasses = rekogInput['FaceDetails'][0]['Sunglasses']['Value']
        beard = rekogInput['FaceDetails'][0]['Beard']['Value']
        # Return string
        gender = str(rekogInput['FaceDetails'][0]['Gender']['Value']).lower()
        emotion = str(rekogInput['FaceDetails'][0]['Emotions'][0]['Type']).lower()
        agelow = str(rekogInput['FaceDetails'][0]['AgeRange']['Low'])
        agehigh = str(rekogInput['FaceDetails'][0]['AgeRange']['High'])

        text = 'I\'m pretty sure this person is ' + gender + ', and it looks like they are ' + emotion + ". They "

        if smiling:
            text += "are smiling, "
        else:
            text += "are not smiling, "

        if eyeglasses or sunglasses:
            text += "are wearing glasses, "
        else:
            text += "are not wearing glasses, "

        if gender == 'male':
            if beard:
                text += "and have a beard. "
            else:
                text += "and do not have a beard. "
        elif gender == 'female':
            if beard:
                text += "and even though I think they are female, I also think they have a beard. Clearly I'm not very good at this."
            else:
                text += "and since they are female, it's pretty obvious they don't have a beard!"

        text += " Also, they are probably between " + agelow + " and " + agehigh + " years old."

        print(text)
        return text

    @staticmethod
    def conjugateAndArticle(input):
        l = len(input)

        if input[l - 1] is 's':
            return 'are'
        elif input in {'people'}:
            return 'are some'
        elif input in {'furniture', 'art', 'computer hardware', 'hardware', 'housing', 'flora', 'grass', 'sky', 'pottery'}:
            return 'is some'
        elif input in {'inflatable'}:
            return 'is something'
        elif input[l - 1] is 'n' and input[l - 2] is 'o' and input[l - 3] is 'i':
            return 'is something to do with'
        else:
            if input[0] in ('a', 'e', 'i', 'o', 'u'):
                print(len(input))
                return 'is an'
            else:
                print(len(input))
                return 'is a'

