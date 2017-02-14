/*global describe:true, it:true, before:true, after:true */

var
	demand    = require('must'),
	semver    = require('semver')
	;
const Beanworker = require('../consumerworker'); 
const fivebeans = require('fivebeans');
const req = require('request');
const co = require('co');
const mc = require('mongodb').MongoClient;
const IndexHandler = require('../consumerhandler');

// Connection URL
var dburl = 'mongodb://as060502:iTo6_s2C@ds139979.mlab.com:39979/currencytesting2017?socketTimeoutMS=5000';
var dstip = '192.168.8.196';
dstip = 'challenge.aftership.net';
var dstport = 11300;
var tb = 'miu060502';
var host = dstip;
var port = dstport;
// Set Options
var opt = {
	dbstr: dburl,
	host: dstip, // The host to listen on
	port: dstport, // the port to listen on
	tube: tb,
	
};
var tube = tb;
var handler = new IndexHandler(opt); 

var option = {
	id: 'worker_1', // The ID of the worker for debugging and tacking
	host: dstip, // The host to listen on
	port: dstport, // the port to listen on
	handlers: {
		'j_type': handler // setting handlers for types
	},
	ignoreDefault: true
};

var job1 = {
	type: 'j_type',
	payload: {
		from: 'USD',
		to: 'HKD'
	}
};

var failedjob1 = {
	type: 'j_type',
	payload: {
		from: 'USD',
		to: 'HKD'
	}
};

describe('ConsumerWorker', function()
{
	var client, consumer, worker, testhandler;

	before(function()
	{
		client = new fivebeans.client(dstip, 11300);
		testhandler = new IndexHandler(opt);
		//remove all FOREX documents
		testhandler.connectdb(
			(err)=>{
				testhandler.dao.collection('Forex').remove({});
				testhandler.dao.close();
			}
		)
		consumer = new Beanworker(option); // Instantiate a worker
		//consumer.start([tb]);
	});

	describe('#connect()', function()
	{
		it('producer creates and saves a connection', function(done)
		{
			client
			 .on('connect', function(){
				 client.stream.must.exist();
				 done();
			 }).on('error', function(err){
				 console.log("error connecting");
				 throw(err);
			 })
			 .on('close', function(){
				console.log('...Closing the tube...');
			 })
			 .connect();
		});
	});
	
	describe('job producer:', function()
	{
		it('#use() connects to a specific tube', function(done)
		{
			client.use(tube, function(err, response)
			{
				demand(err).not.exist();
				response.must.equal(tube);
				done();
			});
		});

		it('#list_tube_used() returns the tube used by a producer', function(done)
		{
			client.list_tube_used(function(err, response)
			{
				demand(err).not.exist();
				response.must.equal(tube);
				done();
			});
		});

		it('#put() submits a job', function(done)
		{
			client.put(0, 0, 60, JSON.stringify(job1), function(err, jobid)
			{
				demand(err).not.exist();
				jobid.must.exist();
				done();
			});
		});

		after(function(done)
		{
			client.stats(function(err, response)
			{
				demand(err).not.exist();
				done();
				client.quit();
			});

		});
	});

	describe('job consumer:', function()
	{
		it('#check if forex records have been inserted', function(done)
		{
			var self = testhandler;
			consumer.on('job.devared',
				(jobid) => {
					demand(jobid).exist();
					//console.log(jobid);
					testhandler.connectdb(function(err)
					{
						self.dao.collection('Forex').count(
							(err, c) => {
								c.must.be.at.least(10);
								console.log(c);
								done();
							}
						)
						self.dao.close();
						
					});
				}
			).start([tb]);
		});
		after(function(done)
		{
			consumer.on('info', function(msg)
				{
					demand(msg).exist();
					//console.log(msg);
					done();
				}
			).stop();

		});
	});

});
