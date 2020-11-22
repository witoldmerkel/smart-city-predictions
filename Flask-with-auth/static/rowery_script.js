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
            var x_stat = document.getElementById("stacje_stat");
            var option = document.createElement("option");
            var option_pred = document.createElement("option");
            var option_stat = document.createElement("option");
            option.text = "Prosze wybrac stację";
            option_pred.text = "Prosze wybrac stację";
            option_stat.text = "Prosze wybrac stację";
            x.add(option);
            x_pred.add(option_pred);
            x_stat.add(option_stat);
            for (i=0; i < response.length; i++){
                var x = document.getElementById("stacje");
                var x_pred = document.getElementById("stacje_pred");
                var x_stat = document.getElementById("stacje_stat");
                var option = document.createElement("option");
                var option_pred = document.createElement("option");
                var option_stat = document.createElement("option");
                option.text = JSON.parse(response[i][0]).name;
                option.value = JSON.parse(response[i][0]).station_id;
                option_pred.text = JSON.parse(response[i][0]).name;
                option_pred.value = JSON.parse(response[i][0]).station_id;
                option_stat.text = JSON.parse(response[i][0]).name;
                option_stat.value = JSON.parse(response[i][0]).station_id;
                x.add(option);
                x_pred.add(option_pred);
                x_stat.add(option_stat);}})};

// Przy załadowaniu okienka dane zostaną wczytane do opcji wyboru
    window.onload = zaladujDane();
// Funkcja pobierająca dane dotyczace wybranego przez użytkownika punktu oraz okresu czasu
// Następnie te dane są ładowane do wygenerowanej tabeli
    var pobierzDane = function () {
        var stacja = $('#stacje').val();
        var fromd = Date.parse($('#from').val())/1000;
        var tod = Date.parse($('#to').val())/1000;
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/velib/dane/" + stacja + "/" + fromd + "/" + tod,
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            var tabela = document.getElementById("tabel");
            tabela.innerHTML='';
            var singleRow=document.createElement('tr');
            singleRow.innerHTML += '<td>' + "Data i godzina" + '</td>';
            singleRow.innerHTML += '<td>' + "Liczba dostępnych rowerów mechanicznych" + '</td>';
            singleRow.innerHTML += '<td>' + "Liczba dostępnych rowerów elektrycznych" + '</td>';
            tabela.appendChild(singleRow);
            for (i=0; i < response.length; i++){
                var singleRow=document.createElement('tr');
                const dateObject = new Date(JSON.parse(response[i][0]).timestamp * 1000)
                const humanDateFormat = dateObject.toLocaleString()
                singleRow.innerHTML += '<td>' + humanDateFormat + '</td>';
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).mechanical + '</td>';
                singleRow.innerHTML += '<td>' + JSON.parse(response[i][0]).ebike + '</td>';
                tabela.appendChild(singleRow);}

})};