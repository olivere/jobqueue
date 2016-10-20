$(function() {

  if (window['WebSocket']) {
    $('#warning').hide();

    var state = {};
    var topics = [];
    var conn = null;
    var connected = false;

    function updateJobList(domid, jobs) {
      $(domid).html('');
      _.each(jobs, function(job) {
        var html = '<li class="list-group-item">';
        if (job.state) {
          html = html + '<span class="badge">' + job.state + '</span>';
        }
        html = html + '<a onclick="lookupJob(\'' + job.id + '\')">' + (job.cid || job.id) + ' in rank ' + job.rank + '</a>';
        html = html + '</li>';
        $(domid).append(html);
      });
    }

    function unixNanoToTime(nanos) {
      if (!nanos) {
        return '';
      }
      var date = new Date(nanos / 1e6);
      return date.toLocaleString();
      /*
      var hh = date.getHours();
      var mm = '0' + date.getMinutes();
      var ss = '0' + date.getSeconds();
      return hh + ':' + mm.substr(-2) + ':' + ss.substr(-2);
      */
    }

    function openConnection() {
      var url = '';
      if (window.location.protocol === 'https:') {
          url = 'wss:';
      } else {
          url = 'ws:';
      }
      url = url + '//' + window.location.host + '/ws';

      conn = new WebSocket(url);

      conn.onopen = function(evt) {
        connected = true;
        $('#CONNECTED').show();
        $('#DISCONNECTED').hide();
      }

      conn.onclose = function(evt) {
        connected = false;
        $('#CONNECTED').hide();
        $('#DISCONNECTED').show();
        setTimeout(openConnection(), 5000);
      }

      conn.onmessage = function(evt) {
        connected = true;
        $('#CONNECTED').show();
        $('#DISCONNECTED').hide();
        if (!evt.data) {
          return;
        }
        var e = JSON.parse(evt.data);
        if (e.job) {
          var topic = e.job.topic;
          if (topics.indexOf(e.job.topic) === -1) {
            topics.push(e.job.topic);
            $('#TOPICS').append('<li class="list-group-item">' + topic + '</li>');
          }
        }
        switch (e.type) {
          case 'SET_STATE':
            var stats = e.stats;
            if (stats) {
              $('#WAITING').html(stats.waiting || 0);
              $('#WORKING').html(stats.working || 0);
              $('#SUCCEEDED').html(stats.succeeded || 0);
              $('#FAILED').html(stats.failed || 0);
            }
            if (e.waiting) {
              updateJobList('#JOBS_WAITING', e.waiting);
            }
            if (e.working) {
              updateJobList('#JOBS_WORKING', e.working);
            }
            if (e.succeeded) {
              updateJobList('#JOBS_SUCCEEDED', e.succeeded);
            }
            if (e.failed) {
              updateJobList('#JOBS_FAILED', e.failed);
            }
            break;
          case 'JOB_LOOKUP':
            if (e.message) {
              $('#JOB_DETAILS_MESSAGE').html(e.message);
            } else {
              $('#JOB_DETAILS_MESSAGE').html('');
            }
            if (e.job) {
              var html = '<table class="table">';
              html += '<tr><td>ID</td><td>' + e.job.id + '</td></tr>';
              html += '<tr><td>Topic</td><td>' + e.job.topic + '</td></tr>';
              html += '<tr><td>State</td><td>' + e.job.state + '</td></tr>';
              html += '<tr><td>Rank</td><td>' + e.job.rank + '</td></tr>';
              html += '<tr><td>CID</td><td>' + e.job.cid + '</td></tr>';
              html += '<tr><td>Created</td><td>' + unixNanoToTime(e.job.created) + '</td></tr>';
              html += '<tr><td>Started</td><td>' + unixNanoToTime(e.job.started) + '</td></tr>';
              html += '<tr><td>Completed</td><td>' + unixNanoToTime(e.job.completed) + '</td></tr>';
              html += '</table>';
              $('#JOB_DETAILS_JOB').html(html);
            } else {
              $('#JOB_DETAILS_JOB').html('');
            }
            $('#JOB_DETAILS_DIALOG').modal();
            break;
        }
      }
    }
    openConnection();

    // lookupJob by id
    window.lookupJob = function(id) {
      if (!connected) {
        return null;
      }
      conn.send(JSON.stringify({
        type: 'JOB_LOOKUP',
        id: id
      }));
    }

  } else {
    $('#warning').show();
  }
});
