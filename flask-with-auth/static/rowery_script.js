// Funkcja odpowiadająca za załadowanie opcji do listy, aby użytkownik mógł wybrać
    var zaladujDane = function () {
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/velib/stacje",
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            var x = document.getElementById("stacje");
            var x_pred = document.getElementById("stacje_pred");
            var option = document.createElement("option");
            var option_pred = document.createElement("option");
            option.text = "Prosze wybrac stację";
            option.value = '0';
            option_pred.text = "Prosze wybrac stację";
            option_pred.value = '0';
            x.add(option);
            x_pred.add(option_pred);
            for (i=0; i < response.length; i++){
                var x = document.getElementById("stacje");
                var x_pred = document.getElementById("stacje_pred");
                var option = document.createElement("option");
                var option_pred = document.createElement("option");
                option.text = JSON.parse(response[i][0]).name;
                option.value = JSON.parse(response[i][0]).station_id;
                option_pred.text = JSON.parse(response[i][0]).name;
                option_pred.value = JSON.parse(response[i][0]).station_id;
                x.add(option);
                x_pred.add(option_pred);}})};

// Przy załadowaniu okienka dane zostaną wczytane do opcji wyboru
    window.onload = zaladujDane();
// Funkcja pobierająca dane dotyczace wybranego przez użytkownika punktu oraz okresu czasu
// Następnie te dane są ładowane do wygenerowanej tabeli
    var pobierzDane = function () {
        $('#loader_hist').removeClass("hide-loader");
        var stacja = $('#stacje').val();
        var fromd = Date.parse($('#from').val())/1000;
        var tod = Date.parse($('#to').val())/1000;
        if(stacja == '0'){
            alert("Proszę wybrać stacje z rowerami - sekcja danych historycznych")
            $('#loader_hist').addClass("hide-loader");
        } else if(fromd > tod){
            alert("Prosze wybrać prawidłowy przedział czasowy - sekcja danych historycznych")
            $('#loader_hist').addClass("hide-loader");
        } else {
            var settings = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/velib/dane/" + stacja + "/" + fromd + "/" + tod,
                "method": "GET",
                "dataType": 'json'
            }
            $.ajax(settings).done(function (response) {
                if (response.length == 0){
                    alert("W bazie danych nie ma, żadnych danych o wybranej specyfikacji - sekcja danych historycznych")
                }
                var tabela = document.getElementById("tabel");
                tabela.innerHTML='';
                var singleRow=document.createElement('tr');
                singleRow.innerHTML += '<td>' + "Data i godzina" + '</td>';
                singleRow.innerHTML += '<td>' + "Liczba dostępnych rowerów mechanicznych" + '</td>';
                singleRow.innerHTML += '<td>' + "Liczba dostępnych rowerów elektrycznych" + '</td>';
                tabela.appendChild(singleRow);
                for (i=0; i < response.length; i++){
                    var singleRow=document.createElement('tr');
                    var dateObject = new Date((JSON.parse(response[i][0]).timestamp - 3600) * 1000);
                    var data = dateObject.toLocaleString();
                    singleRow.innerHTML += '<td>' + data + '</td>';
                    singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).mechanical + '</td>';
                    singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).ebike + '</td>';
                    tabela.appendChild(singleRow);}
                $('#loader_hist').addClass("hide-loader");

})}};

    var pobierzDane_pred = function () {
        $('#loader_pred').removeClass("hide-loader");
        var stacje_pred = $('#stacje_pred').val();
        if(stacje_pred == '0'){
            alert("Proszę wybrać stacje z rowerami - sekcja predykcji")
            $('#loader_pred').addClass("hide-loader");
        } else {
            var poczatek_pred = "'"
            stacje_pred = poczatek_pred.concat(stacje_pred, "'")
            var settings = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/velib/predykcja/" + stacje_pred,
                "method": "GET",
                "dataType": 'json'
            }
            $.ajax(settings).done(function (response) {
                if (response.length == 0){
                    alert("W bazie danych nie ma, żadnych predykcji z wybranej stacji")
                }
                var tabela_pred = document.getElementById("tabel_pred");
                tabela_pred.innerHTML='';
                var singleRow_pred=document.createElement('tr');
                singleRow_pred.innerHTML += '<td>' + "Data i godzina" + '</td>';
                singleRow_pred.innerHTML += '<td>' + "Liczba dostępnych rowerów" + '</td>';
                tabela_pred.appendChild(singleRow_pred);
                for (i=0; i < response.length; i=i+15){
                    var singleRow_pred=document.createElement('tr');
                    var dateObject = new Date((JSON.parse(response[i][0]).timestamp + 14400 - 3600) * 1000);
                    var rok = dateObject.getFullYear();
                    var miesiac = addZero(dateObject.getMonth());
                    var dzien = addZero(dateObject.getDay());
                    var godzina = addZero(dateObject.getHours());
                    var minuty = addZero(dateObject.getMinutes());
                    var data = rok + "/" + miesiac + "/" + dzien + " " + godzina + ":" + minuty;
                    singleRow_pred.innerHTML += '<td>' + data + '</td>';
                    singleRow_pred.innerHTML += '<td>' + Math.round(JSON.parse(response[i][0]).prediction) + '</td>';
                    tabela_pred.appendChild(singleRow_pred);}
                $('#loader_pred').addClass("hide-loader");

})}};

    var pobierzDane_stat = function () {
        $('#loader_stat_dane').removeClass("hide-loader");
        $('#loader_stat_model').removeClass("hide-loader");
        var from_stat = Date.parse($('#from_stat').val())/1000;
        var to_stat = Date.parse($('#to_stat').val())/1000;
        if (from_stat > to_stat){
            alert("Prosze wybrać prawidłowy przedział czasowy - sekcja statystyk");
            $('#loader_stat_dane').addClass("hide-loader");
            $('#loader_stat_model').addClass("hide-loader");
        } else {
            var settings = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/velib/staty/" + from_stat + '/' + to_stat,
                "method": "GET",
                "dataType": 'json'
            }
            $.ajax(settings).done(function (response) {
                if (response.length == 0){
                    alert("W bazie danych nie ma, żadnych danych o wybranej specyfikacji - sekcja statystyki (dane)")
                }
                var tabela_danych = document.getElementById("tabel_danych");
                tabela_danych.innerHTML='';
                var singleRow_danych=document.createElement('tr');
                singleRow_danych.innerHTML += '<td>' + "Średnia liczba dostępnych rowerów" + '</td>';
                singleRow_danych.innerHTML += '<td>' + "Ile danych napłynęło" + '</td>';
                singleRow_danych.innerHTML += '<td>' + "Ile danych nie napłynęło" + '</td>';
                tabela_danych.appendChild(singleRow_danych);
                if (response.length != 0){
                    var liczba_rekordow = 0;
                    var roznica_rekordow = 0;
                    var srednia = 0;
                    for (i=0; i < response.length; i++){
                       liczba_rekordow = liczba_rekordow + JSON.parse(response[i][0]).number_of_records;
                       roznica_rekordow = roznica_rekordow + JSON.parse(response[i][0]).diff;
                       srednia = srednia + JSON.parse(response[i][0]).mean_target;}
                    var singleRow_danych=document.createElement('tr');
                    srednia = srednia/response.length;
                    singleRow_danych.innerHTML += '<td>' + Math.round(srednia * 100) / 100 + '</td>';
                    singleRow_danych.innerHTML += '<td>' + liczba_rekordow + '</td>';
                    singleRow_danych.innerHTML += '<td>' + roznica_rekordow + '</td>';
                    tabela_danych.appendChild(singleRow_danych);


    }
            $('#loader_stat_dane').addClass("hide-loader")})
        var settings1 = {
                    "async": true,
                    "crossDomain": true,
                    "url": "http://127.0.0.1:5000/velib/statymod/" + from_stat + '/' + to_stat,
                    "method": "GET",
                    "dataType": 'json'
                }
                $.ajax(settings1).done(function (response) {
                    if (response.length == 0){
                    alert("W bazie danych nie ma, żadnych danych o wybranej specyfikacji - sekcja statystyki (modele)")
                }
                    var tabela_danych = document.getElementById("tabel_modelu");
                    tabela_danych.innerHTML='';
                    var singleRow_danych=document.createElement('tr');
                    singleRow_danych.innerHTML += '<td>' + "Czas uczenia [s]" + '</td>';
                    singleRow_danych.innerHTML += '<td>' + "Błąd RMSE" + '</td>';
                    singleRow_danych.innerHTML += '<td>' + "Rodzaj" + '</td>';
                    tabela_danych.appendChild(singleRow_danych);
                    if (response.length != 0){
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
                        singleRow_danych.innerHTML += '<td>' + "Regresor" + '</td>';
                        tabela_danych.appendChild(singleRow_danych);
            }
                    $('#loader_stat_model').addClass("hide-loader")})

        }};
    function addZero(i) {
        if (i < 10) {
        i = "0" + i;
      }
        return i;
    }