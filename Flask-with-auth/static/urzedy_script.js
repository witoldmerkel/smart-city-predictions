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
            option_pred.text = "Prosze wybrac urząd";
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
// Funkcja pobierająca dane dotyczace wybranego przez użytkownika punktu oraz okresu czasu
// Następnie te dane są ładowane do wygenerowanej tabeli
    var pobierzDane = function () {
        var urzad = $('#urzedy').val();
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
            var fromd = Date.parse($('#from').val())/1000;
            var tod = Date.parse($('#to').val())/1000;
            var settings = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/urzedy/dane/" + urzad + "/" + fromd + "/" + tod,
                "method": "GET",
                "dataType": 'json'
        }
            $.ajax(settings).done(function (response) {
                var tabela = document.getElementById("tabel");
                tabela.innerHTML='';
                var singleRow=document.createElement('tr');
                singleRow.innerHTML += '<td>' + "Data i godzina" + '</td>';
                singleRow.innerHTML += '<td>' + "Liczba osób w kolejce" + '</td>';
                tabela.appendChild(singleRow);
                for (i=0; i < response.length; i++){
                    var singleRow=document.createElement('tr');
                    const dateObject = new Date((JSON.parse(response[i][0]).timestamp - 3600) * 1000)
                    const humanDateFormat = dateObject.toLocaleString()
                    singleRow.innerHTML += '<td>' + humanDateFormat + '</td>';
                    singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).liczbaklwkolejce + '</td>';
                    tabela.appendChild(singleRow);}

})

})};
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
        var urzad = $('#urzedy_pred').val();
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
            var select = document.getElementById("okienka_pred");
            select.innerHTML = "";
            for (i=0; i < response.length; i++){
                var x = document.getElementById("okienka_pred");
                var option = document.createElement("option");
                option.text = JSON.parse(response[i][0]).okienko;
                option.value = JSON.parse(response[i][0]).okienko;
                x.add(option);}})};

// Funkcja ładująca okienka dla wybranego urzędu w sekscji statystyki
    var zaladujOkna_stat = function(){
        var urzad = $('#urzedy_stat').val();
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
            var select = document.getElementById("okienka_stat");
            select.innerHTML = "";
            for (i=0; i < response.length; i++){
                var x = document.getElementById("okienka_stat");
                var option = document.createElement("option");
                option.text = JSON.parse(response[i][0]).okienko;
                option.value = JSON.parse(response[i][0]).okienko;
                x.add(option);}})};

    var pobierzDane_pred = function () {
        var urzad_pred = $('#urzedy_pred').val();
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
            for (i=0; i < response.length; i++){
                if (JSON.parse(response[i][0]).okienko == okienko_pred){
                    var idgrupy = JSON.parse(response[i][0]).idgrupy;
                }
            }
            var urzad_pred1 = idgrupy
            var poczatek_pred = "'"
            urzad_pred1 = poczatek_pred.concat(urzad_pred1, "'")
            var settings1 = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/urzedy/predykcja/" + urzad_pred1,
                "method": "GET",
                "dataType": 'json'
        }
            $.ajax(settings1).done(function (response) {
                var tabela_pred = document.getElementById("tabel_pred");
                tabela_pred.innerHTML='';
                var singleRow_pred=document.createElement('tr');
                singleRow_pred.innerHTML += '<td>' + "Data i godzina" + '</td>';
                singleRow_pred.innerHTML += '<td>' + "Liczba osób w kolejce" + '</td>';
                tabela_pred.appendChild(singleRow_pred);
                for (i=0; i < response.length; i++){
                    var singleRow_pred = document.createElement('tr');
                    const dateObject = new Date(JSON.parse(response[i][0]).timestamp * 1000)
                    const humanDateFormat = dateObject.toLocaleString()
                    singleRow_pred.innerHTML += '<td>' + humanDateFormat + '</td>';
                    singleRow_pred.innerHTML += '<td>' + Math.round(JSON.parse(response[i][0]).prediction) + '</td>';
                    tabela_pred.appendChild(singleRow_pred);}
            })})};

    var pobierzDane_stat = function () {
        var from_stat = Date.parse($('#from_stat').val())/1000;
        var to_stat = Date.parse($('#to_stat').val())/1000;
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/urzedy/staty/" + from_stat + '/' + to_stat,
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            var tabela_danych = document.getElementById("tabel_danych");
            tabela_danych.innerHTML='';
            var singleRow_danych=document.createElement('tr');
            singleRow_danych.innerHTML += '<td>' + "Średnia długość kolejki" + '</td>';
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
            srednia = srednia/response.length;
            singleRow_danych.innerHTML += '<td>' + Math.round(srednia * 100) / 100 + '</td>';
            singleRow_danych.innerHTML += '<td>' + liczba_rekordow + '</td>';
            singleRow_danych.innerHTML += '<td>' + roznica_rekordow + '</td>';
            tabela_danych.appendChild(singleRow_danych);

})
        var settings1 = {
                "async": true,
                "crossDomain": true,
                "url": "http://127.0.0.1:5000/urzedy/statymod/" + from_stat + '/' + to_stat,
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
                singleRow_danych.innerHTML += '<td>' + "Regresor" + '</td>';
                tabela_danych.appendChild(singleRow_danych);
            })
        };