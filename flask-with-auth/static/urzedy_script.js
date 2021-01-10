// Funkcja odpowiadająca za załadowanie opcji do listy, aby użytkownik mógł wybrać
    var zaladujDane = function () {
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/urzedy/nazwy",
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            var x = document.getElementById("urzedy");
            var x_pred = document.getElementById("urzedy_pred");
            var option = document.createElement("option");
            var option_pred = document.createElement("option");
            option.text = "Prosze wybrac urząd";
            option.value = '0';
            option_pred.text = "Prosze wybrac urząd";
            option_pred.value = '0';
            x.add(option);
            x_pred.add(option_pred);
            for (i=0; i < response.length; i++){
                var x = document.getElementById("urzedy");
                var x_pred = document.getElementById("urzedy_pred");
                var option = document.createElement("option");
                var option_pred = document.createElement("option");
                option.text = JSON.parse(response[i][0]).urzad;
                option.value = JSON.parse(response[i][0]).urzad;
                option_pred.text = JSON.parse(response[i][0]).urzad;
                option_pred.value = JSON.parse(response[i][0]).urzad;
                x.add(option);
                x_pred.add(option_pred);
            }
        })};
// Przy załadowaniu okienka dane zostaną wczytane do opcji wyboru
    window.onload = zaladujDane();

// Jak strona się załaduje to zostanie ustawiony domyślny przedział czasowy
    $(document).ready(function(){
        var tmp = Date.now();
        var tmp_from = tmp - 7*1000*86400;
        var tmp_to = tmp - 1000*86400;
        var data1 = new Date(tmp_from);
        var data2 = new Date(tmp_to)
        var from_date = data1.getFullYear().toString() + '-' + (data1.getMonth() + 1).toString().padStart(2, 0) +
            '-' + data1.getDate().toString().padStart(2, 0);
        var to_date = data2.getFullYear().toString() + '-' + (data2.getMonth() + 1).toString().padStart(2, 0) +
            '-' + data2.getDate().toString().padStart(2, 0);
        $('#from').val(from_date + "T00:00");
        $('#from_stat').val(from_date + "T00:00");
        $('#to').val(to_date + "T00:00");
        $('#to_stat').val(to_date + "T00:00");
    });

// Funkcja pobierająca dane dotyczace wybranego przez użytkownika punktu oraz okresu czasu
// Następnie te dane są ładowane do wygenerowanej tabeli
    var pobierzDane = function () {
        $('#loader_hist').removeClass("hide-loader");
        var urzad = $('#urzedy').val();
        var fromd = Date.parse($('#from').val())/1000;
        var tod = Date.parse($('#to').val())/1000;
        if(urzad == '0'){
            alert("Proszę wybrać urząd - sekcja danych historycznych")
            $('#loader_hist').addClass("hide-loader");
        } else if(fromd > tod){
            alert("Prosze wybrać prawidłowy przedział czasowy - sekcja danych historycznych")
            $('#loader_hist').addClass("hide-loader");
        } else {
            var poczatek = "'"
            urzad = poczatek.concat(urzad, "'")
            var okienko = $('#okienka').val()
            var settings = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/urzedy/pomoc/" + urzad,
                "method": "GET",
                "dataType": 'json'
            }
            $.ajax(settings).done(function (response) {
                for (i=0; i < response.length; i++){
                    if (JSON.parse(response[i][0]).okienko == okienko){
                        var idgrupy = JSON.parse(response[i][0]).idgrupy;
                    }
                }
                var urzad = idgrupy
                var settings = {
                    "async": true,
                    "crossDomain": true,
                    "url": "http://127.0.0.1:5000/urzedy/dane/" + urzad + "/" + fromd + "/" + tod,
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
                    singleRow.innerHTML += '<td>' + "Liczba osób w kolejce" + '</td>';
                    tabela.appendChild(singleRow);
                    for (i=0; i < response.length; i++){
                        var singleRow=document.createElement('tr');
                        var dateObject = new Date((JSON.parse(response[i][0]).timestamp) * 1000);
                        var data = dateObject.toLocaleString();
                        singleRow.innerHTML += '<td>' + data + '</td>';
                        singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).liczbaklwkolejce + '</td>';
                        tabela.appendChild(singleRow);}
                    $('#loader_hist').addClass("hide-loader");

})

})}};
// Funkcja ładująca okienka dla wybranego urzędu w sekcji danych archiwalnych
    var zaladujOkna = function(){
        var urzad = $('#urzedy').val();
        var poczatek = "'"
        urzad = poczatek.concat(urzad, "'")
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/urzedy/" + urzad,
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            var select = document.getElementById("okienka");
            select.innerHTML = "";
            for (i=0; i < response.length; i++){
                var x = document.getElementById("okienka");
                var option = document.createElement("option");
                option.text = JSON.parse(response[i][0]).okienko;
                option.value = JSON.parse(response[i][0]).okienko;
                x.add(option);}})};

// Funkcja ładująca okienka dla wybranego urzędu w sekcji predykcji
    var zaladujOkna_pred = function(){
        var urzad_pred = $('#urzedy_pred').val();
        var poczatek = "'"
        urzad_pred = poczatek.concat(urzad_pred, "'")
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/urzedy/" + urzad_pred,
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            var select = document.getElementById("okienka_pred");
            select.innerHTML = "";
            for (i=0; i < response.length; i++){
                var x = document.getElementById("okienka_pred");
                var option = document.createElement("option");
                option.text = JSON.parse(response[i][0]).okienko;
                option.value = JSON.parse(response[i][0]).okienko;
                x.add(option);}})};


    var pobierzDane_pred = function () {
        $('#loader_pred').removeClass("hide-loader");
        var urzad_pred = $('#urzedy_pred').val();
        if(urzad_pred == '0'){
            alert("Proszę wybrać urząd - sekcja predykcji")
            $('#loader_pred').addClass("hide-loader");
        } else {
            var poczatek = "'"
            urzad_pred = poczatek.concat(urzad_pred, "'")
            var okienko_pred = $('#okienka_pred').val()
            var settings = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/urzedy/pomoc/" + urzad_pred,
                "method": "GET",
                "dataType": 'json'
            }
            $.ajax(settings).done(function (response) {
                if (response.length == 0){
                    alert("W bazie danych nie ma, żadnych predykcji z wybranej stacji")
                }
                for (i=0; i < response.length; i++){
                    if (JSON.parse(response[i][0]).okienko == okienko_pred){
                        var idgrupy = JSON.parse(response[i][0]).idgrupy;
                    }
                }
                var urzad_pred1 = idgrupy
                var czas = Math.round(Date.now()/1000) - 14400;
                var poczatek_pred = "'"
                urzad_pred1 = poczatek_pred.concat(urzad_pred1, "'")
                var settings1 = {
                    "async": true,
                    "crossDomain": true,
                    "url": "http://127.0.0.1:5000/urzedy/predykcja/" + urzad_pred1 + "/" + czas,
                    "method": "GET",
                    "dataType": 'json'
            }
                $.ajax(settings1).done(function (response) {
                    if (response.length == 0){
                    alert("W bazie danych nie ma, żadnych predykcji z wybranego urzędu i okienka")
                }
                    var tabela_pred = document.getElementById("tabel_pred");
                    tabela_pred.innerHTML='';
                    var singleRow_pred=document.createElement('tr');
                    singleRow_pred.innerHTML += '<td>' + "Data i godzina" + '</td>';
                    singleRow_pred.innerHTML += '<td>' + "Liczba osób w kolejce" + '</td>';
                    tabela_pred.appendChild(singleRow_pred);
                    for (i=0; i < response.length; i=i+15){
                        var singleRow_pred = document.createElement('tr');
                        var dateObject = new Date((JSON.parse(response[i][0]).timestamp + 14400)* 1000);
                        var godzina = addZero(dateObject.getHours());
                        var minuty = addZero(dateObject.getMinutes());
                        var data = dateObject.getFullYear().toString() + '/' + (dateObject.getMonth() + 1).toString().padStart(2, 0) +
                        '/' + dateObject.getDate().toString().padStart(2, 0) + " " + godzina + ":" + minuty;
                        singleRow_pred.innerHTML += '<td>' + data + '</td>';
                        singleRow_pred.innerHTML += '<td>' + Math.round(JSON.parse(response[i][0]).prediction) + '</td>';
                        tabela_pred.appendChild(singleRow_pred);}
                    $('#loader_pred').addClass("hide-loader");
            })})}};

    var pobierzDane_stat = function () {
        $('#loader_stat_dane').removeClass("hide-loader");
        $('#loader_stat_model').removeClass("hide-loader");
        var from_stat = Date.parse($('#from_stat').val())/1000;
        var to_stat = Date.parse($('#to_stat').val())/1000;
        if (from_stat > to_stat){
            alert("Prosze wybrać prawidłowy przedział czasowy - sekcja statystyk")
            $('#loader_stat_dane').addClass("hide-loader");
            $('#loader_stat_model').addClass("hide-loader");
        } else {
            var settings = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/urzedy/staty/" + from_stat + '/' + to_stat,
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
                singleRow_danych.innerHTML += '<td>' + "Średnia długość kolejki" + '</td>';
                singleRow_danych.innerHTML += '<td>' + "Ile danych napłynęło" + '</td>';
                singleRow_danych.innerHTML += '<td>' + "Ile danych nie napłynęło" + '</td>';
                tabela_danych.appendChild(singleRow_danych);
                if (response.length != 0) {
                    var liczba_rekordow = 0;
                    var roznica_rekordow = 0;
                    var srednia = 0;
                    for (i = 0; i < response.length; i++) {
                        liczba_rekordow = liczba_rekordow + JSON.parse(response[i][0]).number_of_records;
                        roznica_rekordow = roznica_rekordow + JSON.parse(response[i][0]).diff;
                        srednia = srednia + JSON.parse(response[i][0]).mean_target;
                    }
                    var singleRow_danych = document.createElement('tr');
                    srednia = srednia / response.length;
                    singleRow_danych.innerHTML += '<td>' + Math.round(srednia * 100) / 100 + '</td>';
                    singleRow_danych.innerHTML += '<td>' + liczba_rekordow + '</td>';
                    singleRow_danych.innerHTML += '<td>' + roznica_rekordow + '</td>';
                    tabela_danych.appendChild(singleRow_danych);

                }
            $('#loader_stat_dane').addClass("hide-loader")})
            var settings1 = {
                    "async": true,
                    "crossDomain": true,
                    "url": "http://127.0.0.1:5000/urzedy/statymod/" + from_stat + '/' + to_stat,
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