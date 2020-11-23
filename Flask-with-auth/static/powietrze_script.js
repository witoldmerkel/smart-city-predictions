// Funkcja odpowiadająca za załadowanie opcji do listy, aby użytkownik mógł wybrać
    var zaladujDane = function () {
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/powietrze/nazwy",
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            var x = document.getElementById("miasta");
            var x_pred = document.getElementById("miasta_pred");
            var option = document.createElement("option");
            var option_pred = document.createElement("option");
            option.text = "Prosze wybrac punkt";
            option_pred.text = "Prosze wybrac punkt";
            x.add(option);
            x_pred.add(option_pred);
            for (i=0; i < response.length; i++){
                var x = document.getElementById("miasta");
                var x_pred = document.getElementById("miasta_pred");
                var option = document.createElement("option");
                var option_pred = document.createElement("option");
                option.text = JSON.parse(response[i][0]).nazwa;
                option.value = JSON.parse(response[i][0]).nazwa;
                option_pred.text = JSON.parse(response[i][0]).nazwa;
                option_pred.value = JSON.parse(response[i][0]).nazwa;
                x.add(option);
                x_pred.add(option_pred);}})};

// Przy załadowaniu okienka dane zostaną wczytane do opcji wyboru
    window.onload = zaladujDane();
// Funkcja pobierająca dane dotyczace wybranego przez użytkownika punktu oraz okresu czasu
// Następnie te dane są ładowane do wygenerowanej tabeli

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
            singleRow.innerHTML += '<td>' + "PM2.5 [μg/m3]" + '</td>';
            singleRow.innerHTML += '<td>' + "PM10 [μg/m3]" + '</td>';
            singleRow.innerHTML += '<td>' + "CO [μg/m3]" + '</td>';
            singleRow.innerHTML += '<td>' + "NO2 [μg/m3]" + '</td>';
            singleRow.innerHTML += '<td>' + "O3 [μg/m3]" + '</td>';
            singleRow.innerHTML += '<td>' + "SO2 [μg/m3]" + '</td>';
            singleRow.innerHTML += '<td>' + "Temperatura [°C]" + '</td>';
            singleRow.innerHTML += '<td>' + "Ciśnienie [hPa]" + '</td>';
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
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).pm25 + '</td>';
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).pm10 + '</td>';
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).co + '</td>';
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).no2 + '</td>';
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).o3 + '</td>';
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).so2 + '</td>';
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).t + '</td>';
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).p + '</td>';
                tabela.appendChild(singleRow);}

})};