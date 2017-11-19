function itsAlive() {
    var pollyInput = document.getElementById('pollyinputid').value;
    var pollyVoice = document.getElementById('voice').value;

    if (pollyInput.length === 0) {
       swal("Whoops!", "Looks like you didn't type anything... I'm smarter than that you know!", "warning")
       return;
   }

   $.ajax({
       type: "GET",
       url: '/pollytalk',
       data: {'pollyinput': pollyInput, 'voice': pollyVoice},
       success: function(response){
           var audio = new Audio(response['result']);
           audio.play();
       }
   })
}