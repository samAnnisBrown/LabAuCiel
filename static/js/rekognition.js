// Grab elements, create settings, etc.
//var video = document.getElementById('videoCanvas');
var video = document.querySelector('#videoCanvas');
var cameras = [];
var constraints = {};
var camera_direction = "outside";

// Get access to the camera!
video.setAttribute('autoplay', '');
video.setAttribute('muted', '');
video.setAttribute('playsinline', '');

navigator.mediaDevices.enumerateDevices()
  .then(gotDevices)
  .catch(errorCallback);

  function errorCallback() { console.log('dude, where is my devices');}

  function gotDevices(deviceInfos) {

  for (var i = 0; i !== deviceInfos.length; ++i) {
    var deviceInfo = deviceInfos[i];
    if (deviceInfo.kind === 'videoinput') {
      cameras.push(deviceInfo.deviceId);
      //console.log(deviceInfo);
    }

  }
  constraints = {
    audio: false,
    video: { deviceId: { exact: cameras[0]} }
  }

  navigator.mediaDevices.getUserMedia(constraints).
  then(handleSuccess).catch(handleError);

}

function handleSuccess(stream) {
  window.stream = stream;
  video.srcObject = stream;

  video.addEventListener('playing', function() {
      document.getElementById("videoCanvas").width = this.videoWidth;
      document.getElementById("videoCanvas").height = this.videoHeight;

      document.getElementById("outputCanvas").width = this.videoWidth - 20;
      document.getElementById("outputCanvas").height = this.videoHeight - 20;
  }, false);

  video.play();
}


function handleError(error) {
  console.log('navigator.getUserMedia error: ', error);
}

function rekognise() {
// Get polly voice
    var ios = !!navigator.platform && /iPad|iPhone|iPod/.test(navigator.platform);
    document.getElementById("regoktext").innerHTML = "Gimme a sec... Just trying to figure out what this is!";
    var pollyVoice = document.getElementById('voice').value;

    // Elements for taking the snapshot
    var canvas = document.getElementById('outputCanvas');
    var context = canvas.getContext('2d');
    var video = document.getElementById('videoCanvas');

    context.drawImage(video, 0, 0, canvas.width, canvas.height);

    var image = canvas.toDataURL("image/jpeg");

    // Get details from server
    if (ios) {
        $.ajax({
            type: "POST",
            url: '/rekognise',
            async: false,
            data: {'data': image, 'voice': pollyVoice},
            success: function(response){
                var audio = new Audio(response['result'][0]);
                audio.play();
                document.getElementById("regoktext").innerHTML = response['result'][1];
            }
           })
    } else {
        $.ajax({
        type: "POST",
        url: '/rekognise',
        data: {'data': image, 'voice': pollyVoice},
        success: function(response){
            var audio = new Audio(response['result'][0]);
            audio.play();
            document.getElementById("regoktext").innerHTML = response['result'][1];
        }
       })
    }

}

function changecamera() {
  if (camera_direction == "outside") {
    constraints = {
      audio: false,
      video: { deviceId: { exact: cameras[1]} }
    }
    document.getElementById("videoCanvas").innerHTML = "videoinput:"+cameras[1];

    video.addEventListener('playing', function() {
        document.getElementById("videoCanvas").width = this.videoWidth;
        document.getElementById("videoCanvas").height = this.videoHeight;

        document.getElementById("outputCanvas").width = this.videoWidth - 20;
        document.getElementById("outputCanvas").height = this.videoHeight - 20;
    }, false);

    navigator.mediaDevices.getUserMedia(constraints).
    then(handleSuccess).catch(handleError);
    camera_direction = "selfie";

 } else {
    camera_direction = "outside";
    constraints = {
        audio: false,
        video: { deviceId: { exact: cameras[0]} }
      }
    document.getElementById("videoCanvas").innerHTML = "videoinput:"+cameras[0];

    video.addEventListener('playing', function() {
        document.getElementById("videoCanvas").width = this.videoWidth;
        document.getElementById("videoCanvas").height = this.videoHeight;

        document.getElementById("outputCanvas").width = this.videoWidth - 20;
        document.getElementById("outputCanvas").height = this.videoHeight - 20;
    }, false);

    navigator.mediaDevices.getUserMedia(constraints).
      then(handleSuccess).catch(handleError);
    }
}

function doOnOrientationChange() {
    if (camera_direction = "outside") {
        constraints = {
            audio: false,
            video: { deviceId: { exact: cameras[0]} }
        }
        document.getElementById("videoCanvas").innerHTML = "videoinput:"+cameras[0];

        video.addEventListener('playing', function() {
            document.getElementById("videoCanvas").width = this.videoWidth;
            document.getElementById("videoCanvas").height = this.videoHeight;

            document.getElementById("outputCanvas").width = this.videoWidth - 20;
            document.getElementById("outputCanvas").height = this.videoHeight - 20;
        }, false);

        navigator.mediaDevices.getUserMedia(constraints).
          then(handleSuccess).catch(handleError);
    } else {
        constraints = {
            audio: false,
            video: { deviceId: { exact: cameras[1]} }
        }
        document.getElementById("videoCanvas").innerHTML = "videoinput:"+cameras[1];

        video.addEventListener('playing', function() {
            document.getElementById("videoCanvas").width = this.videoWidth;
            document.getElementById("videoCanvas").height = this.videoHeight;

            document.getElementById("outputCanvas").width = this.videoWidth - 20;
            document.getElementById("outputCanvas").height = this.videoHeight - 20;
        }, false);

        navigator.mediaDevices.getUserMedia(constraints).
          then(handleSuccess).catch(handleError);
    }
}

window.addEventListener('orientationchange', doOnOrientationChange);