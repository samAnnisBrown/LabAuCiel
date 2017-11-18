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
            text = "I don't know what the hell I'm looking at!"
            print(text)

        return polly.toS3(text, voice), text

    @staticmethod
    def detectObject(rekogInput):
        print(rekogInput)
        object = str(rekogInput['Labels'][0]['Name']).lower()
        confidence = str(round(rekogInput['Labels'][0]['Confidence'], 1))

        if len(rekogInput['Labels']) > 1:
            text = "In this image, I am " + confidence + "% sure " + rekog.grammarise(object) + " " + object

            for i in range(1, len(rekogInput['Labels'])):
                object = str(rekogInput['Labels'][i]['Name']).lower()
                confidence = str(round(rekogInput['Labels'][i]['Confidence'], 1))

                if i is len(rekogInput['Labels']) - 1:
                    text += ", and " + confidence + "% sure " + rekog.grammarise(object) + " " + object + "."
                else:
                    text += ", " + confidence + "% sure " + rekog.grammarise(object) + " " + object

        else:
            text = "I'm " + confidence + "% sure what I'm looking at " + rekog.grammarise(object) + " " + object + "."
        print(text)

        return text

    @staticmethod
    def detectCelebrity(rekogInput):
        celebname = rekogInput['CelebrityFaces'][0]['Name']
        celebconfidence = str(round(rekogInput['CelebrityFaces'][0]['Face']['Confidence'], 1))

        text = "This looks like a celebrity!  I'm " + celebconfidence + "% confident that this is " + celebname + "."
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

        text = "I'm pretty sure this person is " + gender + ", and it looks like they're " + emotion + ". They "

        if smiling:
            text += "are smiling, "
        else:
            text += "aren't smiling, "

        if eyeglasses or sunglasses:
            text += "are wearing glasses, "
        else:
            text += "aren't wearing glasses, "

        if gender == 'male':
            if beard:
                text += "and have a beard. "
            else:
                text += "and do not have a beard. "
        elif gender == 'female':
            if beard:
                text += "and even though I think they're female, I also think they have a beard. Clearly I'm not very good at this."
            else:
                text += "and since they're female, it's pretty obvious they don't have a beard!"

        text += "  Also, they are probably between " + agelow + ", and " + agehigh + " years old."

        print(text)
        return text

    @staticmethod
    def grammarise(input):
        l = len(input)

        # ----- Location ----
        if input in {'nature', 'urban'}:
            return 'this is'

        elif input in {'bedroom', 'conference room', 'neighborhood', 'room'}:
            return 'we are in a'

        elif input in {'indoors', 'outdoors'}:
            return 'we are'

        # ----- Collectives ----
        elif input in {'people'}:
            return 'there are some'

        elif input in {'alcohol', 'pavement', 'furniture', 'art', 'computer hardware', 'hardware', 'housing', 'flora', 'grass', 'sky', 'pottery', 'modern art', 'hardwood', 'asphalt', 'tarmac', 'lighting', 'wood'}:
            return 'there is some'

        # ----- Actionable ----
        elif input in {'inflatable'}:
            return 'there is something'

        # ----- Abstract ----
        elif input[l - 1] is 'n' and input[l - 2] is 'o' and input[l - 3] is 'i' or input in {'architecture', 'interior design'}:
            return 'there is'

        # ----- Plural Override -----
        elif input in {'atlas'}:
            return rekog.startsWithVowel(input)

        # ----- Plural ----
        elif input[l - 1] is 's':
            return 'there are'

        else:
            return rekog.startsWithVowel(input)

    @staticmethod
    def startsWithVowel(input):
        if input[0] in ('a', 'e', 'i', 'o', 'u'):
            return 'there is an'
        else:
            return 'there is a'
