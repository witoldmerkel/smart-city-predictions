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
            option.value = '0';
            option_pred.text = "Prosze wybrac punkt";
            option_pred.value = '0';
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
        var fromd = Date.parse($('#from').val())/1000;
        var tod = Date.parse($('#to').val())/1000;
        if(miasto == '0'){
            alert("Proszę wybrać stacje pomiarową - sekcja danych historycznych")
        }  else if(fromd > tod){
            alert("Prosze wybrać prawidłowy przedział czasowy - sekcja danych historycznych")
        } else {
            var poczatek = "'"
            miasto = poczatek.concat(miasto, "'")

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
                    const dateObject = new Date((JSON.parse(response[i][0]).timestamp - 3600) * 1000)
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

})}};


    var pobierzDane_pred = function () {
        var miasto_pred = $('#miasta_pred').val();
        if(miasto_pred == '0'){
            alert("Proszę wybrać stacje pomiarową - sekcja predykcji")
        } else {
            var poczatek_pred = "'"
            miasto_pred = poczatek_pred.concat(miasto_pred, "'")
            var settings = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/powietrze/predykcja/" + miasto_pred,
                "method": "GET",
                "dataType": 'json'
            }
            $.ajax(settings).done(function (response) {
                var tabela_pred = document.getElementById("tabel_pred");
                tabela_pred.innerHTML='';
                var singleRow_pred=document.createElement('tr');
                singleRow_pred.innerHTML += '<td>' + "Data i godzina" + '</td>';
                singleRow_pred.innerHTML += '<td>' + "Stan powietrze" + '</td>';
                tabela_pred.appendChild(singleRow_pred);
                for (i=0; i < response.length; i=i+15){
                    var singleRow_pred=document.createElement('tr');
                    const dateObject = new Date(JSON.parse(response[i][0]).timestamp * 1000)
                    const humanDateFormat = dateObject.toLocaleString()
                    singleRow_pred.innerHTML += '<td>' + humanDateFormat + '</td>';
                    singleRow_pred.innerHTML += '<td>' + JSON.parse(response[i][0]).prediction + '</td>';
                    tabela_pred.appendChild(singleRow_pred);}

})}};

    var pobierzDane_stat = function () {
        var from_stat = Date.parse($('#from_stat').val())/1000;
        var to_stat = Date.parse($('#to_stat').val())/1000;
        if (from_stat > to_stat){
            alert("Prosze wybrać prawidłowy przedział czasowy - sekcja statystyk")
        } else{
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/powietrze/staty/" + from_stat + '/' + to_stat,
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            var tabela_danych = document.getElementById("tabel_danych");
            tabela_danych.innerHTML='';
            var singleRow_danych=document.createElement('tr');
            singleRow_danych.innerHTML += '<td>' + "Najczęściej występujący stan zanieczyszczenia powietrza" + '</td>';
            singleRow_danych.innerHTML += '<td>' + "Ile danych napłyneło" + '</td>';
            singleRow_danych.innerHTML += '<td>' + "Ile danych nie napłyneło" + '</td>';
            tabela_danych.appendChild(singleRow_danych);
            var liczba_rekordow = 0;
            var roznica_rekordow = 0;
            var srednia = 0;
            for (i=0; i < response.length; i++){
               liczba_rekordow = liczba_rekordow + JSON.parse(response[i][0]).number_of_records;
               roznica_rekordow = roznica_rekordow + JSON.parse(response[i][0]).diff;
               srednia = srednia + JSON.parse(response[i][0]).mean_target;}
            var singleRow_danych=document.createElement('tr');
            var stan;
            srednia = srednia / response.length;
                if (srednia < 12) {
                    stan = "Dobre";
                } else if (srednia <= 35) {
                    stan = "Umiarkowane";
                } else if (srednia <= 55) {
                    stan = "Niezdrowe dla chorych";
                } else if (srednia <= 150) {
                    stan = "Niezdrowe";
                } else if (srednia <= 250) {
                    stan = "Bardzo niezdrowe";
                } else {
                    stan = "Niebezpieczne";
                }
            singleRow_danych.innerHTML += '<td>' + stan + '</td>';
            singleRow_danych.innerHTML += '<td>' + liczba_rekordow + '</td>';
            singleRow_danych.innerHTML += '<td>' + roznica_rekordow + '</td>';
            tabela_danych.appendChild(singleRow_danych);

})
        var settings1 = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/powietrze/statymod/" + from_stat + '/' + to_stat,
                "method": "GET",
                "dataType": 'json'
            }
            $.ajax(settings1).done(function (response) {
                var tabela_danych = document.getElementById("tabel_modelu");
                tabela_danych.innerHTML='';
                var singleRow_danych=document.createElement('tr');
                singleRow_danych.innerHTML += '<td>' + "Czas uczenia [s]" + '</td>';
                singleRow_danych.innerHTML += '<td>' + "Dokładność" + '</td>';
                singleRow_danych.innerHTML += '<td>' + "Rodzaj" + '</td>';
                tabela_danych.appendChild(singleRow_danych);
                var sr_czas_ucz = 0;
                var sr_wart_stat = 0;
                for (i=0; i < response.length; i++){
                    sr_czas_ucz = sr_czas_ucz + JSON.parse(response[i][0]).learning_time;
                    sr_wart_stat = sr_wart_stat + JSON.parse(response[i][0]).stat;
                }
                sr_czas_ucz = sr_czas_ucz/response.length;
                sr_wart_stat = sr_wart_stat/response.length;
                var singleRow_danych=document.createElement('tr');
                singleRow_danych.innerHTML += '<td>' + Math.round(sr_czas_ucz * 100) / 100 + '</td>';
                singleRow_danych.innerHTML += '<td>' + Math.round(sr_wart_stat * 100) / 100 + '</td>';
                singleRow_danych.innerHTML += '<td>' + "Klasyfikator" + '</td>';
                tabela_danych.appendChild(singleRow_danych);
            })
        }};
