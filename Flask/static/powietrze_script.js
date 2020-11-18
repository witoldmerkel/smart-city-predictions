    var zaladujDane = function () {
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/powietrze/nazwy",
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            for (i=0; i < response.length; i++){
                var x = document.getElementById("miasta");
                var option = document.createElement("option");
                option.text = response[i][0];
                option.value = response[i][0];
                x.add(option);
            }})};
    window.onload = zaladujDane();

    var pobierzDane = function () {
        var miasto = $('#miasta').val();
        var poczatek = "'"
        miasto = poczatek.concat(miasto, "'")
        var fromd = Date.parse($('#from').val())/1000;
        var tod = Date.parse($('#to').val())/1000;
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/powietrze/dane/" + miasto + "/" + fromd + "/" + tod,
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            var tabela = document.getElementById("tabel");
            tabela.innerHTML='';
            var singleRow=document.createElement('tr');
            singleRow.innerHTML += '<td>' + "Data i godzina" + '</td>';
            singleRow.innerHTML += '<td>' + "Stan powietrze" + '</td>';
            tabela.appendChild(singleRow);
            for (i=0; i < response.length; i++){
                var stan;
                if (JSON.parse(response[i][0]).pm25 < 12) {
                    stan = "Dobre";
                } else if (JSON.parse(response[i][0]).pm25 <= 35) {
                    stan = "Umiarkowane";
                } else if (JSON.parse(response[i][0]).pm25 <= 55) {
                    stan = "Niezdrowe dla chorych";
                } else if (JSON.parse(response[i][0]).pm25 <= 150) {
                    stan = "Niezdrowe";
                } else if (JSON.parse(response[i][0]).pm25 <= 250) {
                    stan = "Bardzo niezdrowe";
                } else {
                    stan = "Niebezpieczne";
                }
                var singleRow=document.createElement('tr');
                const dateObject = new Date(JSON.parse(response[i][0]).timestamp * 1000)
                const humanDateFormat = dateObject.toLocaleString()
                singleRow.innerHTML += '<td>' + humanDateFormat + '</td>';
                singleRow.innerHTML += '<td>' + stan + '</td>';
                tabela.appendChild(singleRow);}

})};