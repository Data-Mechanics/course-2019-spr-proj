const app = document.getElementById('root');

const logo = document.createElement('img');
logo.src = 'logo.png';

const container = document.createElement('div');
container.setAttribute('class', 'container');

app.appendChild(logo);
app.appendChild(container);

var request = new XMLHttpRequest();
request.open('GET', 'https://ghibliapi.herokuapp.com/films', true);
request.onload = function () {

  // // Begin accessing JSON data here
  // var data = JSON.parse(this.response);
  // if (request.status >= 200 && request.status < 400) {
  //   data.forEach(movie => {
  //     const card = document.createElement('div');
  //     card.setAttribute('class', 'card');

  //     const h1 = document.createElement('h1');
  //     h1.textContent = movie.title;

  //     const p = document.createElement('p');
  //     movie.description = movie.description.substring(0, 300);
  //     p.textContent = `${movie.description}...`;

  //     container.appendChild(card);
  //     card.appendChild(h1);
  //     card.appendChild(p);
  //   });
  // } else {
  //   const errorMessage = document.createElement('marquee');
  //   errorMessage.textContent = `Gah, it's not working!`;
  //   app.appendChild(errorMessage);

  // 1. Create the button
  
  var buttonBefore = document.createElement("button");
  buttonBefore.innerHTML = "Bus Stops Before";
  buttonBefore.setAttribute('class', 'card');

  var buttonAfter = document.createElement("button");
  buttonAfter.innerHTML = "Bus Stops After";
  buttonAfter.setAttribute('class', 'card');

  var buttonProperty = document.createElement("button");
  buttonProperty.innerHTML = "Property Assessment";
  buttonProperty.setAttribute('class', 'card');

  // 2. Append somewhere
  var body = document.getElementsByTagName("body")[0];
  body.appendChild(buttonBefore);
  body.appendChild(buttonAfter);
  body.appendChild(buttonProperty);

  // 3. Add event handler
  buttonBefore.addEventListener ("click", function() {
    window.location.href = "mapBefore.html";
  });
  buttonAfter.addEventListener ("click", function() {
    window.location.href = "mapAfter.html";
  });
  buttonProperty.addEventListener ("click", function() {
    $.ajax({
      type:'get',
      url:'/URLToTriggerGetRequestHandler',
      cache:false,
      async:'asynchronous',
      dataType:'json',
      success: function(data) {
        console.log(JSON.stringify(data))
      },
      error: function(request, status, error) {
        console.log("Error: " + error)
      }
   });
  });
  
  }

request.send();