'use strict';

const Beanworker = require('./consumerworker'); // 'fivebeans').worker;
//let Beanworker = require('fivebeans').worker;
const fivebeans = require('fivebeans');
const req = require('request');
const co = require('co');
const mc = require('mongodb').MongoClient;
const IndexHandler = require('./consumerhandler');


// Connection URL
let dburl = 'mongodb://as060502:iTo6_s2C@ds139979.mlab.com:39979/currencytesting2017?socketTimeoutMS=5000';

// Use connect method to connect to the server

let dstip = '192.168.8.196';
dstip = 'challenge.aftership.net';
let dstport = 11300;
let tb = 'miu060502';

// Set Handler Options
/**
 * ConsumerHandler options
 *
 * @param {object} options - options parameters
 * @param {string} options.dbstr - mongodb connection string
 * @param {string} [options.host] - beanstalkd server ip
 * @param {string} [options.tube] - beanstalkd tube name to subscribe
 * @param {number} [options.port]- beanstalkd server port
 * @constructor
 */
let opt = {
	dbstr: dburl,
	host: dstip, // The host to listen on
	port: dstport, // the port to listen on
	tube: tb,
};
// Instantiate the class
let handler = new IndexHandler(opt);

// Set Beanworker options
/**
 * Beanworker options
 *
 * @param {object} options - options parameters
 * @param {object} options.handler - handler object
 * @param {string} [options.host] - beanstalkd server ip
 * @param {number} [options.port]- beanstalkd server port
 * @constructor
 */
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

worker.start([tb])
