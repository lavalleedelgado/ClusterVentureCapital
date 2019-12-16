'use strict';

// Identify cluster labels in the data.
var CLUSTERS = [
	'agriculture', 
	'airlines_and_airports', 
	'business_services',
	'coal_mining',
	'computers',
	'construction', 
	'electric_utilities',
	'lodging_and_conventions', 
	'manufacturing',
	'oil_and_gas',
	'other_banking_and_financial_services',
	'other_energy',
	'other_health_care',
	'other_real_estate',
	'other',
	'restaurants',
	'retailing', 
	'telecommunications', 
	'tourism_and_travel_services'
]

// Collect requirements.
var assert = require('assert');
const filesystem = require('fs');
const http = require('http');
const url = require('url');
const express = require('express');
const mustache = require('mustache');
const hbase = require('hbase-rpc-client');

// Specify connections and the HBase client.
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
const app = express();
app.use(express.static('public'));
app.get('/lookup.html', function (req, res) {

	// Capture and write the user input to the console.
	const year = req.query['year']
	const quarter = req.query['quarter']
	const msa_code = req.query['msa_code']
	console.log('MSA: ' + msa_code + ', PERIOD: ' + year + quarter);

	// Rearrange the parameters to identify HBase keys.
	const msa_id = (year - Math.floor(year / 10)) + '' + msa_code
	const vc_id = year + '' + quarter + '' + msa_code;
	console.log(msa_id)
	
	// Request the MSA label.
	const get_msa_id = new hbase.Get(msa_id);
	client.get('pld_msa_hbase', get_msa_id, function(msa_err, msa_response) {

		// Handle any error.
		assert.ok(!msa_err, 'get returned an error: #{err}');
		if (!msa_response) {
			res.send('<html><body>The requested MSA does not exist in the data.</body></html>');
			return;
		}

		// Print the response to the console.
		console.log('MSA: ' + msa_response);

		// Request the employment and venture capital.
		const get_vc_id = new hbase.Get(vc_id);
    	client.get('pld_vc_wide_hbase', get_vc_id, function(err, vc_response) {

			// Handle any error.
			assert.ok(!err, 'get returned an error: #{err}');
			if (!vc_response) {
				res.send('<html><body>The requested filings do not exist in the data.</body></html>');
				return;
			}

			// Print the response to the console.
			console.log('VC: ' + vc_response);

			// Write a function to safely compute percentages of the data.
			function cluster_pct(cluster, suffix) {
				var cluster = vc_response.cols['vc:' + cluster + '_' + suffix].value
				var msa = vc_response.cols['vc:msa_' + suffix].value
				if (msa != 0) {
					return (cluster / msa * 100).toFixed()
				} else {
					return '—'
				}
			}
			
			// Collect the results.
			var clusters_emp_amt = []
			for (cluster in CLUSTERS) {
				clusters_emp_amt.push(
					{
						cluster_label: cluster,
						cluster_emp: vc_response.cols['vc:' + cluster + '_emp'].value,
						cluster_emp_pct: cluster_pct(cluster, 'emp'),
						cluster_amt: vc_response.cols['vc:' + cluster + '_amt'].value,
						cluster_amt_pct: cluster_pct(cluster, 'amt')
					}
				)
			}
			var view = {
				'msa_label': msa_response.cols['msa:msa_label'].value,
				'clusters' : clusters_emp_amt
			}

			// Map results to the mustache template and push to the web app.
			var template = filesystem.readFileSync('result.mustache').toString();
			var html = mustache.render(template, view);
			res.send(html);
		});
	});
});

// Standby for query.
app.listen(port);
