'use strict';

let fivebeans = require('fivebeans');
let Promise = require('bluebird');
let co = require('co'); 
let dstip = '192.168.8.196'
dstip = 'challenge.aftership.net';
let client = new fivebeans.client(dstip, 11300);
let tube ="miu060502";
let job1 = {
	type: 'j_type',
	payload: {
		from: 'USD',
		to: 'HKD'
	}
};
co( function*(){
		 yield new Promise((resolv, rej) =>{client
		 .on('connect', function(){
			 console.log("connected");
			 resolv("connected");
		 }).on('error', function(err){
			 console.log("error connecting");
			 rej(err);
		 })
		 .on('close', function(){
			console.log('...Closing the tube...');
		 })
		 .connect();
		 });
	}
).then(client.use(tube, (err, name)=>{}))
.then(client.put(0,0,60, JSON.stringify([tube, job1]), function(err, jobid){
		console.log("produced job: "+jobid);
	 }))
.then(client.quit()).catch(err=>{console.log(err); client.quit();});
