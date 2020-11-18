    var zaladujDane = function () {
        var settings = {
            "async": true,
            "crossDomain": true,
            "url": "http://127.0.0.1:5000/urzedy/nazwy",
            "method": "GET",
            "dataType": 'json'
        }
        $.ajax(settings).done(function (response) {
            for (i=0; i < response.length; i++){
                var x = document.getElementById("urzedy_i_okienka");
                var x_pred = document.getElementById("urzedy_i_okienka_pred");
                var x_stat = document.getElementById("urzedy_i_okienka_stat");
                var option = document.createElement("option");
                var option_pred = document.createElement("option");
                var option_stat = document.createElement("option");
                option.text = (JSON.parse(response[i][0]).urzad).concat(": ", JSON.parse(response[i][0]).okienko);
                option.value = JSON.parse(response[i][0]).idgrupy;
                option_pred.text = (JSON.parse(response[i][0]).urzad).concat(": ", JSON.parse(response[i][0]).okienko);
                option_pred.value = JSON.parse(response[i][0]).idgrupy;
                option_stat.text = (JSON.parse(response[i][0]).urzad).concat(": ", JSON.parse(response[i][0]).okienko);
                option_stat.value = JSON.parse(response[i][0]).idgrupy;
                x.add(option);
                x_pred.add(option_pred);
                x_stat.add(option_stat);}})};

    window.onload = zaladujDane();

    var pobierzDane = function () {
        var urzad = $('#urzedy_i_okienka').val();
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

})};