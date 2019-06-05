var DatePicker = new DatePicker('id')

var sidebar = $('#sidebar').sidebar();
var geojsonLayer = 0;

/* Map */
var map = L.map('map').setView([-15.22, -53.23], 5);

var openStreetMapDefault = L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: 'Map data &copy; OpenStreetMap contributors'
});

var openTopoMap = L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
    maxZoom: 17,
    attribution: 'Map data: &copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>, <a href="http://viewfinderpanoramas.org">SRTM</a> | Map style: &copy; <a href="https://opentopomap.org">OpenTopoMap</a> (<a href="https://creativecommons.org/licenses/by-sa/3.0/">CC-BY-SA</a>)'
});

var googleSat = L.tileLayer('http://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}', {
    maxZoom: 20,
    subdomains: ['mt0', 'mt1', 'mt2', 'mt3']
});

var googleHybrid = L.tileLayer('http://{s}.google.com/vt/lyrs=s,h&x={x}&y={y}&z={z}', {
    maxZoom: 20,
    subdomains: ['mt0', 'mt1', 'mt2', 'mt3']
});

var googleStreets = L.tileLayer('http://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {
    maxZoom: 20,
    subdomains: ['mt0', 'mt1', 'mt2', 'mt3']
});

var googleTerrain = L.tileLayer('http://{s}.google.com/vt/lyrs=p&x={x}&y={y}&z={z}', {
    maxZoom: 20,
    subdomains: ['mt0', 'mt1', 'mt2', 'mt3']
});

var baseLayers = {
    'OpenStreetMap': openStreetMapDefault,
    'OpenTopoMap': openTopoMap,
    'Google-Satellite': googleSat,
    'Google-Hybrid': googleHybrid,
    'Google-Streets': googleStreets,
    'Google-Terrain': googleTerrain,
}
baseLayers['OpenStreetMap'].addTo(map);

var options = {
    sortLayers: true,
    collapsed: true
}

var overlayGroup  = {};

for(var key in wrs)
{
    try {
        overlayGroup[key] = L.layerGroup();
        var item = wrs[key];
        L.geoJson(item, {onEachFeature: addOverlay});
    } catch (error) {
    }
}

function addOverlay(feature, layer){
    overlayGroup[feature.properties.wrs].addLayer(layer);    
}


L.control.layers(baseLayers, overlayGroup).addTo(map);

map.zoomControl.setPosition('topright');
map.createPane('bbox').style.zIndex = 320;
map.createPane('geojson').style.zIndex = 350;

var drawnItems = new L.LayerGroup().addTo(map);

var drawControl = new L.Control.Draw({
    draw: {
        polygon: false,
        marker: false,
        circlemarker: false,
        polyline: false,
        circle: false,
        rectangle: {
            shapeOptions: {
                color: '#000',
                opacity: .2,
                fillOpacity: 0.1
            }
        }
    },
    edit: false,
    position: 'topright'
});
map.addControl(drawControl);

map.on('draw:drawstart', function (e) {
    drawnItems.clearLayers();
});

map.on('draw:created', function (e) {
    var layer = e.layer;
    layer.options.pane = 'bbox'
    drawnItems.addLayer(layer);
    $('#bbox').val(layer.getBounds().toBBoxString());
});

L.easyButton({
    id: 'showAllbtn',
    position: 'topright',
    type: 'replace',
    leafletClasses: true,
    states: [{
        stateName: 'show-all',
        onClick: function (button, map) {
            alert('Show All - Under development');
        },
        title: 'show all layers',
        icon: 'fa-globe'
    }]
}).addTo(map);

function onEachFeature(feature, layer) {
    layer._leaflet_id = feature.properties.title;
    layer.setStyle({ fillOpacity: 0.01, opacity: 0.8 });
    layer.bindPopup(`<b>${feature.properties.title}</b><br>
            <table class="table">
            <tbody>
              <tr>
                <th scope="row">Date</th>
                <td>${feature.properties.date}</td>
              </tr>
              <tr>
                <th scope="row">Path</th>
                <td>${feature.properties.path}</td>
              </tr>
              <tr>
                <th scope="row">Row</th>
                <td>${feature.properties.row}</td>
              </tr>
              <tr>
                <th scope="row">Satellite</th>
                <td>${feature.properties.satellite}</td>
              </tr>
              <tr>
                <th scope="row">Sensor</th>
                <td>${feature.properties.sensor}</td>
              </tr>
              <tr>
                <th scope="row">Provider</th>
                <td>${feature.properties.provider}</td>
              </tr>
            </tbody>
          </table>`);
    layer.on('click', function (e) {
        layer.closePopup();
        $('#' + feature.properties.title + '_ql').find('span').toggleClass('fa-eye-slash fa-eye');
        if (layer._quicklook) {
            map.removeLayer(layer._quicklook);
            layer._quicklook = null;
        } else {
            var imgUrl = feature.properties.icon;
            var anchor = [[feature.properties.tl_latitude, feature.properties.tl_longitude],
            [feature.properties.tr_latitude, feature.properties.tr_longitude],
            [feature.properties.br_latitude, feature.properties.br_longitude],
            [feature.properties.bl_latitude, feature.properties.bl_longitude]]
            layer._quicklook = L.imageTransform(imgUrl, anchor).addTo(map).bringToFront();
        }
    });
    layer.on('contextmenu', function (e) {
        layer.openPopup();
    });
    layer.on('remove', function (e) {
        if (layer._quicklook) {
            map.removeLayer(layer._quicklook);
            layer._quicklook = null;
        }
    });
}

/* Search */

$(function () {
    $('#searchForm').on('submit', function (event) {
        event.preventDefault();
        var submit = $('#searchSubmit');
        var loadingText = '<i class="fa fa-circle-o-notch fa-spin"></i> Loading...';
        if (submit.html() !== loadingText) {
            submit.data('original-text', submit.html());
            submit.html(loadingText);
        }
        $('#accordion-results').empty();

        $.ajax({
            url: host_url + 'query',
            type: 'get',
            data: $(this).serialize() + '&providers=' + JSON.stringify(checkedProviders),
            dataType: "json",
            success: function (data) {
                $.each(checkedProviders, function (key, data) {
                    $('#accordion-results').append(`<div class="card">
                                                        <div class="card-header card-collapse" id="heading${key}" data-toggle="collapse" data-target="#collapse${key}">
                                                            <h5>${key} <span class="badge badge-primary badge-right" id="badge${key}">0</span></h5>
                                                        </div>
                                                        <div id="collapse${key}" class="collapse show">
                                                            <div id="resultList${key}"></div>
                                                        </div>
                                                    </div>`);
                });
                map.removeLayer(geojsonLayer);
                geojsonLayer = L.geoJson(data, {
                    onEachFeature: onEachFeature,
                    pane: 'geojson'
                }).addTo(map);
                $.each(data.features, function (key, feature) {
                    var prop = feature.properties;
                    var card = `<div class="margin-tb">
                                            <div class="card">
                                                <div class="row"> 
                                                    <div class="col-4">
                                                        <img class="w-100" src="${prop.icon}" >
                                                    </div>
                                                    <div class="col-8 nopadding-left">
                                                        <div class="card-body nopadding-left">
                                                            <p class="card-title"><b>${prop.title}</b></p>
                                                            <div class="btn-group">
                                                                <button type="button" class="btn btn-light quicklook" value="${prop.title}" id=${prop.title}_ql data-toggle="tooltip" data-placement="top" title="Show quicklook"><span class="fa fa-eye-slash"></span></button>
                                                                <button type="button" class="btn btn-light info" value="${prop.title}" id=${prop.title}_info data-toggle="tooltip" data-placement="top" title="Show info card"><span class="fa fa-info"></span></button>
                                                                <button type="button" class="btn btn-light centralize" value="${prop.title}" id=${prop.title}_center data-toggle="tooltip" data-placement="top" title="Centralize to quicklook"><span class="fa fa-dot-circle-o"></span></button>
                                                                <a role="button" class="btn btn-light" id=${prop.title} data-toggle="modal" data-target="#modal" target="_blank" data-toggle="tooltip" data-placement="top" title="Show download list"><span class="fa fa-download"></span></a
                                                            </div>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>`;
                    $('#resultList' + prop.provider).append(card);
                    $('#' + prop.title).click(prop.enclosure, function (object) {
                        $('#modalBody').empty();
                        var itemsList = $('<div/>').addClass('list-group');
                        $.each(object.data, function (index, value) {
                            var color = "#FFFFFF"
                            switch (value.band) {
                                case 'blue':
                                    color = "#A9D0F5"
                                    break;
                                case 'green':
                                    color = "#CCE5CC"
                                    break;
                                case 'red':
                                    color = "#FFCCCC"
                                    break;
                            }
                            var item = `<a href="${value.url}" class="list-group-item list-group-item-action flex-column align-items-start" style="background-color: ${color}">
                                                    <div class="d-flex w-100 justify-content-between">
                                                    <h5 class="mb-1">${prop.title}</h5>
                                                    <small class="text-muted">${value.type}</small>
                                                        </div>
                                                        <p class="mb-1">${value.band}</p>
                                                    <div class="d-flex w-100 justify-content-between">
                                                        <small class="text-muted">Radiometric Processing ${value.radiometric_processing}</small>
                                                    <i class="fa fa-download"></i>
                                                    </div>
                                                </a>`;
                            itemsList.append(item);
                        });
                        $('#modalBody').append(itemsList);
                        $("[data-toggle='popover'").popover();
                    });
                });
                $.each(data.providers, function (key, data) {
                    $("#badge" + data.provider).text(data.totalResults);
                });
                submit.html(submit.data('original-text'));
            }
        });
    });
});

$(document).on('click', '.quicklook', function () {
    var layer = geojsonLayer.getLayer($(this).attr('value'));
    layer.fireEvent('click');
    layer.bringToFront();
});

$(document).on('click', '.info', function () {
    var layer = geojsonLayer.getLayer($(this).attr('value'));
    layer.openPopup();
});

$(document).on('click', '.centralize', function () {
    var layer = geojsonLayer.getLayer($(this).attr('value'));
    map.fitBounds(layer.getBounds());
});


var checkedProviders = {}
$.each(providers, function (key, data) {
    var status = 0;
    $.ajax({
        url: data.url,
        success: function (response) {
            checkedProviders[key] = data;
            $('#providersCheck').append(`<div class="form-check">
                                    <input class="form-check-input" type="checkbox" value="${data.url}" id="${key}" checked>
                                    <label class="form-check-label" for="defaultCheck1">${key}
                                    </label></div>`);
            $("#" + key).click(function () {
                if ($(this).is(":checked")) {
                    checkedProviders[key] = data;
                } else {
                    delete checkedProviders[key];
                }
            });

            if (data.type == 'opensearch') {
                fillSearchDropdowns(response)
            };
        },
        error: function (response) {

            $('#providersCheck').append(`<div class="form-check">
                                    <input class="form-check-input" type="checkbox" value="${data.url}" id="${key}" disabled>
                                    <label class="form-check-label" for="defaultCheck1">${key}
                                    </label></div>`);


        }
    });
});

$(document).ready(function () {
    option = `<option value="" selected="selected"></option>`
    $(option).appendTo($('#bandSelect'));
    $(option).appendTo($('#collectionIdSelect'));
    $(option).appendTo($('#radiometricProcessingSelect'));
    $(option).appendTo($('#typeSelect'));
});

function fillSearchDropdowns(xml) {
    $(xml).find('OpenSearchDescription').each(function () {
        $(this).find('Url').each(function () {
            $(this).find('Parameter').each(function () {
                var name = $(this).attr("name")
                if (name == "dataset") {
                    $(this).find("Option").each(function () {
                        var value = $(this).attr("value");
                        $('<option />', { value: value, text: value }).appendTo($('#collectionIdSelect'));
                    })
                } else if (name == "band") {
                    $(this).find("Option").each(function () {
                        var value = $(this).attr("value");
                        $('<option />', { value: value, text: value }).appendTo($('#bandSelect'));
                    })
                } else if (name == "radiometricProcessing") {
                    $(this).find("Option").each(function () {
                        var value = $(this).attr("value");
                        $('<option />', { value: value, text: value }).appendTo($('#radiometricProcessingSelect'));
                    })
                } else if (name == "type") {
                    $(this).find("Option").each(function () {
                        var value = $(this).attr("value");
                        $('<option />', { value: value, text: value }).appendTo($('#typeSelect'));
                    })
                }
            })
        })
    })
    $(".custom-select option").each(function () {
        var $option = $(this);
        $option.siblings()
              .filter( function(){ return $(this).val() == $option.val() } )
              .remove()
       })
}

