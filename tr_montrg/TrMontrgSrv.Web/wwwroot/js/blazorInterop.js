var blazorInterop = blazorInterop || {};

blazorInterop.initDateRangePicker = () => {
    $('input[name="datetimes"]').daterangepicker({
        timePicker: true,
        startDate: moment().startOf('hour').subtract(24, 'hour'),
        endDate: moment().startOf('hour'),
        "maxSpan": {
            "days": 7
        },
        "minYear": 2021,
        "minDate": "2021/8/28",
        "maxDate": moment().add(1, 'day').format('YYYY/M/DD'),
        locale: {
            //format: 'YYYY/M/DD hh:mm A'
            format: 'YYYY/M/DD HH:mm'
        },
        showDropdowns: true,
        timePicker24Hour: true,
        ranges: {
            'Today': [moment().startOf('day'), moment()],
            'Yesterday': [moment().subtract(1, 'days').startOf('day'), moment().subtract(1, 'days').endOf('day')],
            '2 Days Ago': [moment().subtract(2, 'days').startOf('day'), moment().subtract(2, 'days').endOf('day')],
            'Last 24 Hours': [moment().subtract(1, 'days'), moment()],
            'Last 2 Days': [moment().subtract(2, 'days'), moment()],
            'Last 5 Days': [moment().subtract(5, 'days'), moment()],
            'Last 7 Days': [moment().subtract(7, 'days'), moment()],
            //'Last 30 Days': [moment().subtract(29, 'days'), moment()],
            //'This Month': [moment().startOf('month'), moment().endOf('month')],
            //'Last Month': [moment().subtract(1, 'month').startOf('month'), moment().subtract(1, 'month').endOf('month')]
        }
    });
};

blazorInterop.getInputValue = (id) => {
    return document.getElementById(id).value;
};

blazorInterop.focusElement = (element) => {
    element.focus();
}

blazorInterop.focusElementById = (id) => {
    var element = document.getElementById(id);
    if (element) element.focus();
};

function formatDate(date) {
    var d = new Date(date),
        month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = '' + d.getFullYear(),
        hour = '' + d.getHours(),
        minute = '' + d.getMinutes(),
        second = '' + d.getSeconds();

    if (month.length < 2)
        month = '0' + month;
    if (day.length < 2)
        day = '0' + day;
    if (hour.length < 2)
        hour = '0' + hour;
    if (minute.length < 2)
        minute = '0' + minute;
    if (second.length < 2)
        second = '0' + second;

    return [year, month, day].join('/') + ' ' + [hour, minute, second].join(':');
}

function formatDateToYMDHMS(date) {
    var d = new Date(date),
        month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = '' + d.getFullYear(),
        hour = '' + d.getHours(),
        minute = '' + d.getMinutes(),
        second = '' + d.getSeconds();

    if (month.length < 2)
        month = '0' + month;
    if (day.length < 2)
        day = '0' + day;
    if (hour.length < 2)
        hour = '0' + hour;
    if (minute.length < 2)
        minute = '0' + minute;
    if (second.length < 2)
        second = '0' + second;

    return [year, month, day].join('') + '_' + [hour, minute, second].join('');
}

// ------ Charts -------------------------------------------------------------------------------------------------

blazorInterop.drawCtrChart = (id, data, chartType, url, deviceId) => {
    // clear first
    $('#' + id).empty();

    var isMaterial = false;

    if (isMaterial)
        google.charts.load('current', { 'packages': ['line'] });
    else
        google.charts.load('current', { 'packages': ['corechart'] });

    google.charts.setOnLoadCallback(drawLineGoogleChart);

    function drawLineGoogleChart() {
        // to avoid redrawing with previous data when window resize
        if (data.length == 0) {
            $('#' + id).empty();
            return;
        }

        var dataTable = new google.visualization.DataTable();

        var tooltip = function (dt, v, type) {
            if (type == 'x' || type == 'mr')
                return "<div class='text-secondary'>" + formatDate(dt) + "<br/><div><span class='fs-5 fw-bold'>" + v + "</span> &#8451;</div></div>";
            else if (type == 'xbar')
                return "<div class='text-secondary'>X-bar(x&#772;)<br/><div><span class='fs-5 fw-bold'>" + v.toFixed(1) + "</span> &#8451;</div></div>";
            else if (type == 'mrbar')
                return "<div class='text-secondary'>MR-bar<br/><div><span class='fs-5 fw-bold'>" + v.toFixed(1) + "</span> &#8451;</div></div>";
            else if (type == 'ucl')
                return "<div class='text-secondary'>UCL<br/><div><span class='fs-5 fw-bold'>" + v.toFixed(1) + "</span> &#8451;</div></div>";
            else if (type == 'lcl')
                return "<div class='text-secondary'>LCL<br/><div><span class='fs-5 fw-bold'>" + v.toFixed(1) + "</span> &#8451;</div></div>";
        }

        if (chartType == 'i-max') {
            dataTable.addColumn('datetime', 'DateTime');
            dataTable.addColumn('number', 'Temp.');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'X-bar');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'UCL');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'LCL');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            for (var i = 0; i < data.length; i++) {
                var dt = new Date(Date.parse(data[i].dt));
                var x = data[i].tmax;
                var xbar = data[i].xbar_max;
                var ucl = data[i].ucl_i_max;
                var lcl = data[i].lcl_i_max;
                dataTable.addRows([[dt, x, tooltip(dt, x, 'x'), xbar, tooltip(dt, xbar, 'xbar'), ucl, tooltip(dt, ucl, 'ucl'), lcl, tooltip(dt, lcl, 'lcl')]]);
            }

            if (isMaterial)
                var options = {
                    chart: { title: 'I Chart', subtitle: 'based on max. temperature' },
                    axes: {
                        x: { 0: { label: 'Time' } },
                        y: { 0: { label: 'Individual Value (℃)' } }
                    },
                    colors: ['#5B9BD5', '#77A93B', '#CC9800', '#CC9800'],
                    tooltip: { isHtml: true }
                };
            else
                var options = {
                    title: 'I Chart',
                    titleTextStyle: {
                        color: 'gray'
                    },
                    hAxis: {
                        title: 'Time',
                        titleTextStyle: {
                            color: 'gray',
                            italic: false
                        },
                        textStyle: { color: 'gray' },
                        gridlines: { color: 'transparent' }
                    },
                    vAxis: {
                        title: 'Individual Value  (℃)',
                        titleTextStyle: {
                            color: 'gray',
                            italic: false
                        },
                        textStyle: { color: 'gray' },
                        gridlines: { color: 'transparent' }
                    },
                    series: {
                        0: { lineWidth: 2 },
                        1: { lineWidth: 1 },
                        2: { lineWidth: 1 },
                        3: { lineWidth: 1 }
                    },
                    colors: ['#5B9BD5', '#77A93B', '#CC9800', '#CC9800'],
                    tooltip: { isHtml: true },
                    chartArea: {
                        backgroundColor: '#fafafa'
                    },
                    explorer: {
                        actions: ['dragToZoom', 'rightClickToReset'],
                        axis: 'horizontal',
                        keepInBounds: true,
                        maxZoomIn: 4.0
                    }
                };
        }
        else if (chartType == 'mr-max') {
            dataTable.addColumn('datetime', 'DateTime');
            dataTable.addColumn('number', 'Temp.');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'MR-bar');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'UCL');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'LCL');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            for (var i = 0; i < data.length; i++) {
                var dt = new Date(Date.parse(data[i].dt));
                var mr = data[i].mr_max;
                var mrbar = data[i].mr_bar_max;
                var ucl = data[i].ucl_mr_max;
                var lcl = data[i].lcl_mr_max;
                dataTable.addRows([[dt, mr, tooltip(dt, mr, 'mr'), mrbar, tooltip(dt, mrbar, 'mrbar'), ucl, tooltip(dt, ucl, 'ucl'), lcl, tooltip(dt, lcl, 'lcl')]]);
            }

            if (isMaterial)
                var options = {
                    chart: { title: 'MR Chart', subtitle: 'based on absolute value' },
                    axes: {
                        x: { 0: { label: 'Time' } },
                        y: { 0: { label: 'Moving Range (℃)' } }
                    },
                    colors: ['#5B9BD5', '#77A93B', '#CC9800', '#CC9800']
                };
            else
                var options = {
                    title: 'MR Chart',
                    titleTextStyle: {
                        color: 'gray'
                    },
                    hAxis: {
                        title: 'Time',
                        titleTextStyle: {
                            color: 'gray',
                            italic: false
                        },
                        textStyle: { color: 'gray' },
                        gridlines: { color: 'transparent' }
                    },
                    vAxis: {
                        title: 'Moving Range (℃)',
                        titleTextStyle: {
                            color: 'gray',
                            italic: false
                        },
                        textStyle: { color: 'gray' },
                        gridlines: { color: 'transparent' }
                    },
                    series: {
                        0: { lineWidth: 2 },
                        1: { lineWidth: 1 },
                        2: { lineWidth: 1 },
                        3: { lineWidth: 1 }
                    },
                    colors: ['#5B9BD5', '#77A93B', '#CC9800', '#CC9800'],
                    tooltip: { isHtml: true },
                    chartArea: {
                        backgroundColor: '#fafafa'
                    },
                    explorer: {
                        actions: ['dragToZoom', 'rightClickToReset'],
                        axis: 'horizontal',
                        keepInBounds: true,
                        maxZoomIn: 4.0
                    }
                };
        }
        else if (chartType == 'i-diff') {
            dataTable.addColumn('datetime', 'DateTime');
            dataTable.addColumn('number', 'Diff. Temp.');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'X-bar');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'UCL');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'LCL');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            for (var i = 0; i < data.length; i++) {
                var dt = new Date(Date.parse(data[i].dt));
                var x = data[i].diff;
                var xbar = data[i].xbar_diff;
                var ucl = data[i].ucl_i_diff;
                var lcl = data[i].lcl_i_diff;
                dataTable.addRows([[dt, x, tooltip(dt, x, 'x'), xbar, tooltip(dt, xbar, 'xbar'), ucl, tooltip(dt, ucl, 'ucl'), lcl, tooltip(dt, lcl, 'lcl')]]);
            }

            if (isMaterial)
                var options = {
                    chart: { title: 'I Chart', subtitle: 'based on difference of temperature = max. - ambient(frame min.)' },
                    axes: {
                        x: { 0: { label: 'Time' } },
                        y: { 0: { label: 'Individual Value (℃)' } }
                    },
                    colors: ['#5B9BD5', '#77A93B', '#CC9800', '#CC9800']
                };
            else
                var options = {
                    title: 'I Chart',
                    titleTextStyle: {
                        color: 'gray'
                    },
                    hAxis: {
                        title: 'Time',
                        titleTextStyle: {
                            color: 'gray',
                            italic: false
                        },
                        textStyle: { color: 'gray' },
                        gridlines: { color: 'transparent' }
                    },
                    vAxis: {
                        title: 'Individual Value  (℃)',
                        titleTextStyle: {
                            color: 'gray',
                            italic: false
                        },
                        textStyle: { color: 'gray' },
                        gridlines: { color: 'transparent' }
                    },
                    series: {
                        0: { lineWidth: 2 },
                        1: { lineWidth: 1 },
                        2: { lineWidth: 1 },
                        3: { lineWidth: 1 }
                    },
                    colors: ['#5B9BD5', '#77A93B', '#CC9800', '#CC9800'],
                    tooltip: { isHtml: true },
                    chartArea: {
                        backgroundColor: '#fafafa'
                    },
                    explorer: {
                        actions: ['dragToZoom', 'rightClickToReset'],
                        axis: 'horizontal',
                        keepInBounds: true,
                        maxZoomIn: 4.0
                    }
                };
        }
        else if (chartType == 'mr-diff') {
            dataTable.addColumn('datetime', 'DateTime');
            dataTable.addColumn('number', 'Diff. Temp.');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'MR-bar');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'UCL');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            dataTable.addColumn('number', 'LCL');
            dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
            for (var i = 0; i < data.length; i++) {
                var dt = new Date(Date.parse(data[i].dt));
                var mr = data[i].mr_diff;
                var mrbar = data[i].mr_bar_diff;
                var ucl = data[i].ucl_mr_diff;
                var lcl = data[i].lcl_mr_diff;
                dataTable.addRows([[dt, mr, tooltip(dt, mr, 'mr'), mrbar, tooltip(dt, mrbar, 'mrbar'), ucl, tooltip(dt, ucl, 'ucl'), lcl, tooltip(dt, lcl, 'lcl')]]);
            }

            if (isMaterial)
                var options = {
                    chart: { title: 'MR Chart', subtitle: 'based on absolute value' },
                    axes: {
                        x: { 0: { label: 'Time' } },
                        y: { 0: { label: 'Moving Range (℃)' } }
                    },
                    colors: ['#5B9BD5', '#77A93B', '#CC9800', '#CC9800']
                };
            else
                var options = {
                    title: 'MR Chart',
                    titleTextStyle: {
                        color: 'gray'
                    },
                    hAxis: {
                        title: 'Time',
                        titleTextStyle: {
                            color: 'gray',
                            italic: false
                        },
                        textStyle: { color: 'gray' },
                        gridlines: { color: 'transparent' }
                    },
                    vAxis: {
                        title: 'Moving Range (℃)',
                        titleTextStyle: {
                            color: 'gray',
                            italic: false
                        },
                        textStyle: { color: 'gray' },
                        gridlines: { color: 'transparent' }
                    },
                    series: {
                        0: { lineWidth: 2 },
                        1: { lineWidth: 1 },
                        2: { lineWidth: 1 },
                        3: { lineWidth: 1 }
                    },
                    colors: ['#5B9BD5', '#77A93B', '#CC9800', '#CC9800'],
                    tooltip: { isHtml: true },
                    chartArea: {
                        backgroundColor: '#fafafa'
                    },
                    explorer: {
                        actions: ['dragToZoom', 'rightClickToReset'],
                        axis: 'horizontal',
                        keepInBounds: true,
                        maxZoomIn: 4.0
                    }
                };
        }

        if (isMaterial)
            var chart = new google.charts.Line(document.getElementById(id));
        else
            var chart = new google.visualization.LineChart(document.getElementById(id));

        var dayOfWeek = function (date) {
            var day = date.getDay();
            if (day == 0)
                return 'Sun';
            else if (day == 1)
                return 'Mon';
            else if (day == 2)
                return 'Tue';
            else if (day == 3)
                return 'Wed';
            else if (day == 4)
                return 'Thu';
            else if (day == 5)
                return 'Fri';
            else if (day == 6)
                return 'Sat';
            else
                return 'Unknown';
        };

        function selectHandler() {
            var selectedItem = chart.getSelection()[0];
            if (selectedItem) {
                var dt = dataTable.getValue(selectedItem.row, 0);
                var title = "<span>" + formatDate(dt) + " (" + dayOfWeek(dt) + ")</span>";
                var apiUrl = url + '/' + deviceId + '/' + formatDateToYMDHMS(dt);
                blazorInterop.loadSnap(apiUrl);
                blazorInterop.showModal('snapModalWindow', title, null);
            }
        }
        google.visualization.events.addListener(chart, 'select', selectHandler);

        chart.draw(dataTable, options);
    }

    // Create trigger to resizeEnd event
    $(window).resize(function () {
        if (this.resizeTO) clearTimeout(this.resizeTO);
        this.resizeTO = setTimeout(function () {
            $(this).trigger('resizeEnd');
        }, 500);
    });

    // redraw graph when window resize is completed
    $(window).on('resizeEnd', function () {
        drawLineGoogleChart();
    });
};

blazorInterop.drawMaxPosChart = (id, data, tableId) => {
    // clear first
    $('#' + id).empty();

    var isMaterial = false;

    if (isMaterial)
        google.charts.load('current', { 'packages': ['scatter'] });
    else
        google.charts.load('current', { 'packages': ['corechart'] });

    if (tableId)
        google.charts.load('current', { 'packages': ['table'] });

    google.charts.setOnLoadCallback(drawMaxPosGoogleChart);

    function drawMaxPosGoogleChart() {
        // to avoid redrawing with previous data when window resize
        if (data.length == 0) {
            $('#' + id).empty();
            return;
        }

        var dataTable = new google.visualization.DataTable();
        dataTable.addColumn('number', 'X');
        dataTable.addColumn('number', 'Y');
        dataTable.addColumn({ type: 'string', role: 'tooltip', 'p': { 'html': true } });
        for (var i = 0; i < data.length; i++) {
            var x = data[i].tMaxX;
            var y = data[i].tMaxY;
            var dt = data[i].captureDt;
            var tmax = data[i].tMax;
            var tdiff = data[i].tDiff;
            var tooltip = "<div class='text-secondary'>" + formatDate(dt) + "<br/>" +
                "<div>Max. <span class='fs-5 fw-bold'>" + tmax + "</span> &#8451;</div>" +
                "<div>Diff. <span class='fs-5 fw-bold'>" + tdiff + "</span> &#8451;</div>" +
                "</div>";
            dataTable.addRows([[x, y, tooltip]]);
        }

        if (isMaterial)
            var options = {
                chart: { title: 'Max. Temperature Position', subtitle: '' },
                axes: {
                    x: { 0: { label: 'X' } },
                    y: { 0: { label: 'Y' } }
                },
                colors: ['#5B9BD5', '#77A93B', '#CC9800', '#CC9800'],
                width: 800,
                height: 600
            };
        else
            var options = {
                hAxis: {
                    //textPosition: 'none',
                    textStyle: { color: 'yellow' },
                    gridlines: { color: 'transparent' },
                    minValue: 0,
                    maxValue: 800
                },
                vAxis: {
                    //textPosition: 'none',
                    textStyle: { color: 'yellow' },
                    gridlines: { color: 'transparent' },
                    minValue: 0,
                    maxValue: 600,
                    direction: -1
                },
                colors: ['red', '#77A93B', '#CC9800', '#CC9800'],
                tooltip: { isHtml: true },
                backgroundColor: 'transparent',
                chartArea: {
                    backgroundColor: 'transparent'
                },
                //width: 800,
                //height: 600,
                theme: 'maximized',
                dataOpacity: 0.3,
                pointSize: 10,
                legend: 'none',
                crosshair: { trigger: 'both', color: 'yellow', opacity: 0.7 },
            };

        if (isMaterial)
            var chart = new google.charts.Scatter(document.getElementById(id));
        else
            var chart = new google.visualization.ScatterChart(document.getElementById(id));

        chart.draw(dataTable, options);

        if (tableId) {
            var table = new google.visualization.Table(document.getElementById(tableId));
            table.draw(dataTable, { showRowNumber: false, width: '800', allowHtml: true, pageSize: 10 });

            google.visualization.events.addListener(table, 'select',
                function () {
                    chart.setSelection(table.getSelection());
                });

            google.visualization.events.addListener(chart, 'select',
                function () {
                    table.setSelection(chart.getSelection());
                });
        }


    }


}

// ------ Snap ---------------------------------------------------------------------------------------------------

blazorInterop.loadSnap = (url) => {
    $('#spinner').show();
    $.ajaxSetup({
        cache: false
    });
    $.ajax({
        type: 'GET',
        url: url,
        dataType: 'json',
        contentType: 'application/json;charset=UTF-8',
        success: function (response) {
            //console.table([response]);
            blazorInterop.displaySnap(response);
        }
    }).done(function () {
        $('#spinner').hide();
    }).fail(function (data, textStatus) {
        alert('Request failed: ' + textStatus);
        $('#spinner').hide();
    });
}

blazorInterop.displaySnap = (data) => {
    //$("#plant_id").text(data.plantId);
    //$("#location_id").text(data.locationId);
    //$("#device_id").text(data.deviceId);

    $("#rgbImage").attr("src", "data:image/jpg;base64," + data.media[0].fileContent);
    $("#irImage").attr("src", "data:image/jpg;base64," + data.media[1].fileContent);

    //var year = data.frames[0].ymd.substring(0, 4);
    //var month = data.frames[0].ymd.substring(4, 6);
    //var date = data.frames[0].ymd.substring(6, 8);
    //var hour = data.frames[0].hms.substring(0, 2);
    //var minute = data.frames[0].hms.substring(2, 4);
    //var second = data.frames[0].hms.substring(4, 6);
    //$("#timestamp").text(year + "-" + month + "-" + date + " " + hour + ":" + minute + ":" + second);

    var threshold_temp = 40;
    var threshold_temp_of_roi = 0;
    var warning_temp = 60;
    var diff_good_temp = [0, 10];
    var diff_ok_temp = [10, 20];
    var diff_attention_temp = [20, 40];
    var diff_dangerous_temp = [40, 1000];

    // inner function to append data table row
    var appendDffTemp = function (id, t) {
        if (t > diff_good_temp[0] && t <= diff_good_temp[1]) {
            $("#" + id + " > tbody > tr:last-child").append("<td class='flex-even' style='background-color:rgb(248, 249, 250)'>" + t + " &#8451;</td>");
        } else if (t > diff_ok_temp[0] && t <= diff_ok_temp[1]) {
            $("#" + id + " > tbody > tr:last-child").append("<td class='flex-even text-white' style='background-color:rgb(40, 167, 69)'>" + t + " &#8451;</td>");
        } else if (t > diff_attention_temp[0] && t <= diff_attention_temp[1]) {
            $("#" + id + " > tbody > tr:last-child").append("<td class='flex-even text-white' style='background-color:rgb(255, 193, 7)'>" + t + " &#8451;</td>");
        } else if (t > diff_dangerous_temp[0]) {
            $("#" + id + " > tbody > tr:last-child").append("<td class='flex-even text-white' style='background-color:rgb(220, 53, 69)'>" + t + " &#8451;</td>");
        } else {
            $("#" + id + " > tbody > tr:last-child").append("<td class='flex-even '>" + t + " &#8451;</td>");
        }
    }
    // inner function to append data table row
    var appendTemp = function (id, t) {
        if (t >= warning_temp) {
            $("#" + id + " > tbody > tr:last-child").append("<td class='flex-even bg-danger text-white'>" + t + " &#8451;</td>");
        } else {
            $("#" + id + " > tbody > tr:last-child").append("<td class='flex-even '>" + t + " &#8451;</td>");
        }
    }

    // Frame
    $("#frame_info > tbody").empty();
    $("#frame_info > tbody").append("<tr class='d-flex text-center'><th class='col-2 fw-normal table-dark pr-2'>Frame</th></tr>");
    $.each(data.frames, function (index, item) {
        appendTemp('frame_info', item.tAvg);
        appendTemp('frame_info', item.tMax);
        appendTemp('frame_info', item.tMin);
        appendDffTemp('frame_info', item.tDiff);
        appendTemp('frame_info', item.t90th);
    });

    // ROIs
    $("#roi_info > tbody").empty();
    $.each(data.rois, function (index, item) {
        $("#roi_info > tbody").append("<tr class='d-flex text-center'></tr>");
        $("#roi_info > tbody > tr:last-child").append("<th class='col-2 fw-normal table-dark pr-2' scope='row'>ROI " + item.roiId + "</th>");
        appendTemp('roi_info', item.tAvg);
        appendTemp('roi_info', item.tMax);
        appendTemp('roi_info', item.tMin);
        appendDffTemp('roi_info', item.tDiff);
        appendTemp('roi_info', item.t90th);
    });

    // Boxes
    $("#box_info > tbody").empty();
    $.each(data.boxes, function (index, item) {
        $("#box_info > tbody").append("<tr class='d-flex text-center'></tr>");
        $("#box_info > tbody > tr:last-child").append("<th class='col-2 fw-normal table-dark pr-2' scope='row'>" + item.boxId + "</th>");
        appendTemp('box_info', item.tAvg);
        appendTemp('box_info', item.tMax);
        appendTemp('box_info', item.tMin);
        appendDffTemp('box_info', item.tDiff);
        appendTemp('box_info', item.t90th);
    });
}

// ------ Modal Windows ------------------------------------------------------------------------------------------

blazorInterop.showModal = (id, title, message) => {
    var theModal = new bootstrap.Modal(document.getElementById(id), { backdrop: true, keyboard: true, focus: true });

    var modalHtml = document.getElementById(id);
    var modalTitle = modalHtml.querySelector('.modal-title');
    var modalBody = modalHtml.querySelector('.modal-body');

    modalTitle.innerHTML = title;
    if (message)
        modalBody.innerHTML = message;
    theModal.show();
};
blazorInterop.hideModal = (id) => {
    var theModal = bootstrap.Modal.getInstance(document.getElementById(id));
    theModal.hide();
};

blazorInterop.showSpinner = (id) => {
    if (id == null)
        id = 'spinner';
    //var spinner = document.getElementById(id);
    //spinner.show();
    $('#' + id).show();
};

blazorInterop.hideSpinner = (id) => {
   // $('#spinner').hide();
    if (id == null)
        id = 'spinner';
    //var spinner = document.getElementById(id);
    //spinner.hide();
    $('#' + id).hide();
};

// ---------------------------------------------------------------------------------------------------------------

blazorInterop.openPopupWindowCenter = (pageURL, title, w, h) => {
    var left = (screen.width - w) / 2;
    var top = (screen.height - h) / 4;
    window.open(pageURL, title, 'toolbar=no, location=no, directories=no, status=no, menubar=no, scrollbars=yes, resizable=yes, copyhistory=no, width=' + w + ', height=' + h + ', left=' + left + ', top=' + top);
}
