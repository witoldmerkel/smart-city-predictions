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
            var x_stat = document.getElementById("urzedy_stat");
            var option = document.createElement("option");
            var option_pred = document.createElement("option");
            var option_stat = document.createElement("option");
            option.text = "Prosze wybrac urząd";
            option_pred.text = "Prosze wybrac urząd";
            option_stat.text = "Prosze wybrac urząd";
            x.add(option);
            x_pred.add(option_pred);
            x_stat.add(option_stat);
            for (i=0; i < response.length; i++){
                var x = document.getElementById("urzedy");
                var x_pred = document.getElementById("urzedy_pred");
                var x_stat = document.getElementById("urzedy_stat");
                var option = document.createElement("option");
                var option_pred = document.createElement("option");
                var option_stat = document.createElement("option");
                option.text = JSON.parse(response[i][0]).urzad;
                option.value = JSON.parse(response[i][0]).urzad;
                option_pred.text = JSON.parse(response[i][0]).urzad;
                option_pred.value = JSON.parse(response[i][0]).urzad;
                option_stat.text = JSON.parse(response[i][0]).urzad;
                option_stat.value = JSON.parse(response[i][0]).urzad;
                x.add(option);
                x_pred.add(option_pred);
                x_stat.add(option_stat);}})};

    window.onload = zaladujDane();


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
                    const dateObject = new Date(JSON.parse(response[i][0]).timestamp * 1000)
                    const humanDateFormat = dateObject.toLocaleString()
                    singleRow.innerHTML += '<td>' + humanDateFormat + '</td>';
                    singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).liczbaklwkolejce + '</td>';
                    tabela.appendChild(singleRow);}

})

})};

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