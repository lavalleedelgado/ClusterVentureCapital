'use strict';
const http = require('http');
var assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const hbase = require('hbase-rpc-client');
const hostname = '127.0.0.1';
const port = 3886;

var client = hbase({
    zookeeperHosts: ['mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:2181'],
    zookeeperRoot: '/hbase-unsecure'
});

client.on('error', function(err) {
  console.log(err)
})

// Answer the query for venture capital by cluster for an MSA.
app.use(express.static('public'));
app.get('/lookup.html', function (req, res) {

	// Capture the user input and write to the console.
    const pld_vc_id = req.query['year'] + req.query['quarter'] + req.query['msa_code'];
	console.log(pld_vc_id);
	
	// Request the data from HBase.
    const get = new hbase.Get(pld_vc_id);
    client.get("pld_venture_capital", get, function(err, row) {

		// Handle any error.
		assert.ok(!err, "get returned an error: #{err}");
		if(!row){
			res.send("<html><body>The requested data do not exist.</body></html>");
			return;
		}

		// Print response to the console.
		console.log(row);

		// Map results to the mustache template.
		var template = filesystem.readFileSync('result.mustache').toString();
		var cluster_emp_pct = (
			row.query['pld_vc:cluster_emp'].value / row.query['pld_vc:msa_emp'].value
		)
		var cluster_amt_pct = (
			row.query['pld_vc:cluster_amt'].value / row.query['pld_vc:msa_amt'].value
		)
		var view = {
			'msa_label': row.cols['pld_vc:msa_label'].value,
			'clusters': [
				{
					'cluster_label': row.cols['pld_vc:cluster_label'].value,
					'cluster_emp': row.query['pld_vc:cluster_emp'].value,
					'cluster_emp_pct': (cluster_emp_pct * 100).toFixed(),
					'cluster_amt': req.query['pld_vc:cluster_amt'].value,
					'cluster_amt_pct': (cluster_amt_pct * 100).toFixed()
				}
			]
		};
		var html = mustache.render(template, view);

		// Push the results to the web app.
		res.send(html);
    });
});

// Standby for query.
app.listen(port);
