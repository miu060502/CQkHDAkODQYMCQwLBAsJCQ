'use strict';

const Beanworker = require('./consumerworker'); // 'fivebeans').worker;
const fivebeans = require('fivebeans');
const req = require('request');
const co = require('co');
const mc = require('mongodb').MongoClient;


// Connection URL
const dburl = 'mongodb://as060502:iTo6_s2C@ds139979.mlab.com:39979/currencytesting2017?socketTimeoutMS=5000';

// Use connect method to connect to the server

let dstip = '192.168.8.196';
const dstport = 11300;
const tb = 'miu060502';
const successStep = 1000;
const failStep = 100;
const maxSuccess = 10000;
const maxFail = 300;
const failDelay = 3;
const successDelay = 3;

// dstpip = 'challenge.aftership.net';

let record = {
	'from': 'AAA',
	'to': 'BBB',
	'created_at': new Date(1347772624825),
	'rate': '0.1387897'
};


// Create a class to handle the work load
class IndexHandler {
	connectdb(cb) {
		let self = this;
		let opts = self.opts;

		mc.connect(opts.dbstr, function (err, db) {
			if (err) { console.log('error connecting db'); self.dao = null; return; }

			console.log('Connected successfully to db');
			self.dao = db;
			cb(err);
		});
	}
	connectMQ(cb) {
		let self = this;
		let opts = self.opts;
		self.client = new fivebeans.client(opts.host, opts.port);
		self.client
		.on('connect', function () {
			console.log('beanstalkd connected');
			cb(null);
		}).on('error', function (err) {
			console.log('error connecting beanstalkd');
			self.client.quit();
			cb(err);
		})
		.on('close', function () {
			console.log('...Closing the tube...');
		})
		.connect();
	}
	
	/**
     * IndexHandler constructor
     *
     * @param {object} options - options parameters
     * @param {string} options.dbstr - mongodb connection string
     * @param {string} [options.id] - handler id
     * @param {string} [options.host] - beanstalkd server ip
	 * @param {string} [options.tube] - beanstalkd tube name to subscribe
     * @param {number} [options.port]- beanstalkd server port
     * @constructor
     */
	constructor(options) {
		this.type = options.type; // Specify the type of job for this class to work on
		this.dao;
		this.client;
		this.opts = options;
		this.stat;
	}

  // Define the work to perform and pass back a success
	work(jobID, payload, callback) {
		let self = this;
		co(function* () {
			yield new Promise(function (res, rej) {
				self.connectMQ((err) => { if (err) { rej('err connecting beanstalkd'); } else res('beanstalkd connected'); });
			});
		}
	)
	.then(val => {
		//console.log(val);
		return new Promise(
				(res, rej) => {
					req('http://download.finance.yahoo.com/d/quotes.csv?s=' + payload.from + payload.to + '=X&f=sl1d1t1ba&e=.csv',
						(error, response, body) => {
							if (!error && response.statusCode === 200) {
								if (body.indexOf('N/A') !== -1 || body.length === 0) {
									rej('invalid currency');
									return;
								}

								record.rate = body.replace('"', '').split(',')[1];
								record.from = body.replace('"', '').substring(3, 6);
								record._id = Math.floor(Date.now() / 1000) + record.from + record.to;
								record.to = body.replace('"', '').substring(0, 3);
								record.created_at = Math.floor(Date.now() / 1000);
								self.connectdb((err) => {
									if (err) {
										rej('no db');
										return;
									}
									self.dao.collection('Forex').insert([record],
										(derr, result) => {
											if (derr) {
												console.log(derr);
												rej('error inserting db');
												self.dao.close();
												self.dao = null;
												return;
											}
											self.dao.close();
											self.dao = null;
											res('inserted successfully');
										}
									);
								});
							} else {
								rej('invalid response');
							}
						});
				}
			);
	}
	)
	.then((val) => {
		console.log(val);
		return new Promise(
			(res, rej) => {
				self.client.stats_job(jobID, (err, data) => {
					self.stat = data;
					// console.log(data);
					if (data.pri + successStep >= maxSuccess) {
						callback('success');
					} else						{ callback('release', successDelay, data.pri + successStep); }

					self.client.quit();
					self.client = null;
				});
			});
	}
	).catch(err => {
		console.log(err);
		self.client.stats_job(jobID, (serr, data) => {
			self.stat = data;
			// console.log(data);
			if ((data.pri + successStep) % 1000 >= maxFail) {
				callback('success');
			} else				{ callback('release', failDelay, data.pri + failStep); }

			self.client.quit();
			self.client = null;
		});
	});
	}
}
// Set Options
let opt = {
	dbstr: dburl,
	host: dstip, // The host to listen on
	port: dstport, // the port to listen on
	tube: tb,
	type: 'j_type'
};
// Instantiate the class
let handler = new IndexHandler(opt);

// Set options

let option = {
	id: 'worker_1', // The ID of the worker for debugging and tacking
	host: dstip, // The host to listen on
	port: dstport, // the port to listen on
	handlers: {
		'j_type': handler // setting handlers for types
	},
	ignoreDefault: true
};

let worker = new Beanworker(option); // Instantiate a worker

worker.start([tb]);
