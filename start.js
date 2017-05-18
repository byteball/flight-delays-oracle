/*jslint node: true */
"use strict";
var moment = require('moment');
var request = require('request');
var _ = require('lodash');
var conf = require('byteballcore/conf.js');
var db = require('byteballcore/db.js');
var eventBus = require('byteballcore/event_bus.js');
var headlessWallet = require('headless-byteball');
var desktopApp = require('byteballcore/desktop_app.js');
var objectHash = require('byteballcore/object_hash.js');
var notifications = require('./notifications.js');

if (conf.bRunWitness)
	require('byteball-witness');

const MAX_REQUESTS_PER_DAY = 100;
const MAX_REQUESTS_PER_DEVICE_PER_DAY = 10;

const RETRY_TIMEOUT = 5*60*1000;
const TAXI_IN_TIME = 15*60*1000; // 15 mins
var assocQueuedDataFeeds = {};
var assocDeviceAddressesByFeedName = {};

const WITNESSING_COST = 600; // size of typical witnessing unit
var my_address;
var count_witnessings_available = 0;

if (!conf.bSingleAddress)
	throw Error('oracle must be single address');

if (!conf.bRunWitness)
	headlessWallet.setupChatEventHandlers();

// this duplicates witness code if we are also running a witness
function readNumberOfWitnessingsAvailable(handleNumber){
	count_witnessings_available--;
	if (count_witnessings_available > conf.MIN_AVAILABLE_WITNESSINGS)
		return handleNumber(count_witnessings_available);
	db.query(
		"SELECT COUNT(*) AS count_big_outputs FROM outputs JOIN units USING(unit) \n\
		WHERE address=? AND is_stable=1 AND amount>=? AND asset IS NULL AND is_spent=0", 
		[my_address, WITNESSING_COST], 
		function(rows){
			var count_big_outputs = rows[0].count_big_outputs;
			db.query(
				"SELECT SUM(amount) AS total FROM outputs JOIN units USING(unit) \n\
				WHERE address=? AND is_stable=1 AND amount<? AND asset IS NULL AND is_spent=0 \n\
				UNION \n\
				SELECT SUM(amount) AS total FROM witnessing_outputs \n\
				WHERE address=? AND is_spent=0 \n\
				UNION \n\
				SELECT SUM(amount) AS total FROM headers_commission_outputs \n\
				WHERE address=? AND is_spent=0", 
				[my_address, WITNESSING_COST, my_address, my_address], 
				function(rows){
					var total = rows.reduce(function(prev, row){ return (prev + row.total); }, 0);
					var count_witnessings_paid_by_small_outputs_and_commissions = Math.round(total / WITNESSING_COST);
					count_witnessings_available = count_big_outputs + count_witnessings_paid_by_small_outputs_and_commissions;
					handleNumber(count_witnessings_available);
				}
			);
		}
	);
}


// make sure we never run out of spendable (stable) outputs. Keep the number above a threshold, and if it drops below, produce more outputs than consume.
function createOptimalOutputs(handleOutputs){
	var arrOutputs = [{amount: 0, address: my_address}];
	readNumberOfWitnessingsAvailable(function(count){
		if (count > conf.MIN_AVAILABLE_WITNESSINGS)
			return handleOutputs(arrOutputs);
		// try to split the biggest output in two
		db.query(
			"SELECT amount FROM outputs JOIN units USING(unit) \n\
			WHERE address=? AND is_stable=1 AND amount>=? AND asset IS NULL AND is_spent=0 \n\
			ORDER BY amount DESC LIMIT 1", 
			[my_address, 2*WITNESSING_COST],
			function(rows){
				if (rows.length === 0){
					notifications.notifyAdminAboutPostingProblem('only '+count+" spendable outputs left, and can't add more");
					return handleOutputs(arrOutputs);
				}
				var amount = rows[0].amount;
			//	notifications.notifyAdminAboutPostingProblem('only '+count+" spendable outputs left, will split an output of "+amount);
				arrOutputs.push({amount: Math.round(amount/2), address: my_address});
				handleOutputs(arrOutputs);
			}
		);
	});
}



////////


function postDataFeed(datafeed, onDone){
	function onError(err){
		notifications.notifyAdminAboutFailedPosting(err);
		onDone(err);
	}
	var network = require('byteballcore/network.js');
	var composer = require('byteballcore/composer.js');
	createOptimalOutputs(function(arrOutputs){
		let params = {
			paying_addresses: [my_address], 
			outputs: arrOutputs, 
			signer: headlessWallet.signer, 
			callbacks: composer.getSavingCallbacks({
				ifNotEnoughFunds: onError,
				ifError: onError,
				ifOk: function(objJoint){
					network.broadcastJoint(objJoint);
					onDone();
				}
			})
		};
		if (conf.bPostTimestamp)
			datafeed.timestamp = Date.now();
		let objMessage = {
			app: "data_feed",
			payload_location: "inline",
			payload_hash: objectHash.getBase64Hash(datafeed),
			payload: datafeed
		};
		params.messages = [objMessage];
		composer.composeJoint(params);
	});
}

function reliablyPostDataFeed(datafeed, device_address){
	var feed_name;
	for (var key in datafeed){
		if (key.indexOf('-remark') === -1){
			feed_name = key;
			break;
		}
	}
	if (!feed_name)
		throw Error('no feed name');
	if (device_address){
		if (!assocDeviceAddressesByFeedName[feed_name])
			assocDeviceAddressesByFeedName[feed_name] = [device_address];
		else
			assocDeviceAddressesByFeedName[feed_name].push(device_address);
	}
	if (assocQueuedDataFeeds[feed_name]) // already queued
		return console.log(feed_name+" already queued");
	assocQueuedDataFeeds[feed_name] = datafeed;
	var onDataFeedResult = function(err){
		if (err){
			console.log('will retry posting the data feed later');
			setTimeout(function(){
				postDataFeed(datafeed, onDataFeedResult);
			}, RETRY_TIMEOUT + Math.round(Math.random()*3000));
		}
		else
			delete assocQueuedDataFeeds[feed_name];
	};
	postDataFeed(datafeed, onDataFeedResult);
}


function readExistingData(feed_name, device_address, handleResult){
	let remark_feed_name = feed_name+'-remark';
	if (assocQueuedDataFeeds[feed_name]){
		assocDeviceAddressesByFeedName[feed_name].push(device_address);
		return handleResult(assocQueuedDataFeeds[feed_name][feed_name], assocQueuedDataFeeds[feed_name][remark_feed_name], 0);
	}
	db.query(
		"SELECT feed_name, int_value, `value`, is_stable \n\
		FROM data_feeds CROSS JOIN unit_authors USING(unit) CROSS JOIN units USING(unit) \n\
		WHERE address=? AND feed_name IN(?,?)", 
		[my_address, feed_name, remark_feed_name],
		function(rows){
			if (rows.length === 0)
				return handleResult();
			if (!rows[0].is_stable){
				if (!assocDeviceAddressesByFeedName[feed_name])
					assocDeviceAddressesByFeedName[feed_name] = [device_address];
				else
					assocDeviceAddressesByFeedName[feed_name].push(device_address);
			}
			var data = {};
			rows.forEach(row => {
				data[row.feed_name] = row.value || row.int_value;
			});
			handleResult(data[feed_name], data[remark_feed_name], rows[0].is_stable);
		}
	);
}

function checkRequestQuota(device_address, handleResult){
	db.query("SELECT COUNT(*) AS count FROM fd_responses WHERE device_address=? AND creation_date > "+db.addTime("-1 DAY"), [device_address], function(rows){
		if (rows[0].count > MAX_REQUESTS_PER_DEVICE_PER_DAY){
			notifications.notifyAdmin("too many requests from "+device_address, rows[0].count+" requests today from "+device_address);
			return handleResult("Too many requests today, try again tomorrow");
		}
		db.query("SELECT COUNT(*) AS count FROM fd_responses WHERE creation_date > "+db.addTime("-1 DAY"), function(rows){
			if (rows[0].count > MAX_REQUESTS_PER_DAY){
				notifications.notifyAdmin("too many requests", rows[0].count+" requests today");
				return handleResult("Too many requests today, try again tomorrow");
			}
			handleResult();
		});
	});
}

function getDelayText(delay, remark, bInDb, browser_url){
	var est_text = "";
	if (remark === 'runway')
		est_text = " (estimated based on runway arrival time)";
	else if (remark)
		est_text = " ("+remark+")";
	var text;
	if (delay > 0)
		text = "Arrival delay was "+delay+" minutes"+est_text+".";
	else if (delay < 0)
		text = "The flight arrived "+(-delay)+" minutes early"+est_text+".";
	else
		text = "The flight arrived exactly on time"+est_text+".";
	text += bInDb 
		? "\n\nThe data is already in the database, you can unlock your smart contract now." 
		: "\n\nThe data will be added into the database, I'll let you know when it is confirmed and you are able to unlock your contract.";
	text += "\n\n"+browser_url;
	return text;
}

function getInstruction(){
	return "Please type the flight number and date in DD.MM.YYYY format, e.g. BA950 "+moment().subtract(1, 'days').format('DD.MM.YYYY');
}

function getHelpText(){
	return "This oracle can query the status of any flight finished less than 1 week ago and post its delay status to Byteball database.  You can use this data to unlock a smart contract.  Type the flight number and date in DD.MM.YYYY format, e.g.\n\nBA950 "+moment().subtract(1, 'days').format('DD.MM.YYYY');
}

function status2text(flightStatus){
	switch (flightStatus){
		case 'C': return 'canceled';
		case 'D': return 'diverted';
		case 'R': return 'redirected';
		default: return flightStatus;
	}
}

eventBus.on('paired', function(from_address){
	var device = require('byteballcore/device.js');
	device.sendMessageToDevice(from_address, 'text', getHelpText());
});

eventBus.on('text', function(from_address, text){
	var device = require('byteballcore/device.js');
	text = text.trim();
	let uc_text = text.toUpperCase();

	if (uc_text === 'HELP')
		return device.sendMessageToDevice(from_address, 'text', getHelpText());
	
	let arrDateMatches = uc_text.match(/(\d\d)\.(\d\d)\.(\d\d\d\d)/);
	let text_without_date = arrDateMatches ? uc_text.replace(arrDateMatches[0], '') : uc_text;
	let arrFlightMatches = text_without_date.match(/\b([A-Z0-9]{2})\s*(\d{1,4}[A-Z]?)\b/);
	
	if (!arrDateMatches && !arrFlightMatches)
		return device.sendMessageToDevice(from_address, 'text', "This doesn't look like flight number and date.  "+getInstruction());

	if (!arrDateMatches)
		return device.sendMessageToDevice(from_address, 'text', "Can't find a valid date.  "+getInstruction());

	if (!arrFlightMatches)
		return device.sendMessageToDevice(from_address, 'text', "Can't find a valid flight number.  "+getInstruction());
	
	let date = arrDateMatches[0];
	let m = moment(date, 'DD.MM.YYYY');
	if (!m.isValid())
		return device.sendMessageToDevice(from_address, 'text', "Looks like the date is not valid.  "+getInstruction());
	if (m.isAfter())
		return device.sendMessageToDevice(from_address, 'text', "The date must be in the past.  Only finished flights can be queried.");
	if (m.isBefore(moment().subtract(7, 'days'), 'day'))
		return device.sendMessageToDevice(from_address, 'text', "The flight must be less than 7 days ago.");
	let year = m.year();
	let month = m.month() + 1;
	let day = m.date();
	
	let airline = arrFlightMatches[1];
	let flight_number = arrFlightMatches[2];
	let full_flight = airline+flight_number+" on "+date;
	let feed_name = airline+flight_number+'-'+m.format('YYYY-MM-DD');
	
	let browser_url = "http://www.flightstats.com/go/FlightStatus/flightStatusByFlight.do?airline="+airline+"&flightNumber="+flight_number+"&departureDate="+m.format('YYYY-MM-DD');
	
	readExistingData(feed_name, from_address, function(delay, remark, bStable){
		if (typeof delay === 'number')
			return device.sendMessageToDevice(from_address, 'text', getDelayText(delay, remark, bStable, browser_url));

		checkRequestQuota(from_address, function(err){
			if (err)
				return device.sendMessageToDevice(from_address, 'text', err);
			
			let url = "https://api.flightstats.com/flex/flightstatus/rest/v2/json/flight/status/"+airline+"/"+flight_number+"/dep/"+year+"/"+month+"/"+day+"?appId="+conf.flightstatsAppId+"&appKey="+conf.flightstatsAppKey+"&utc=false";
			console.log('will request '+url);

			request(url, function(error, response, body){
				if (error || response.statusCode !== 200){
					notifications.notifyAdminAboutPostingProblem("getting flightstats data for "+full_flight+" failed: "+error+", status="+response.statusCode);
					return device.sendMessageToDevice(from_address, 'text', "Failed to fetch flightstats data.");
				}
				console.log('response:\n'+body);
				db.query("INSERT INTO fd_responses (device_address, feed_name, response) VALUES(?,?,?)", [from_address, feed_name, body], function(){});
				let jsonResult = JSON.parse(body);
				if (jsonResult.error && jsonResult.error.errorMessage){
					notifications.notifyAdminAboutPostingProblem("error from flightstats: "+body);
					return device.sendMessageToDevice(from_address, 'text', "Error from flightstats: "+jsonResult.error.errorMessage);
				}
				let arrStatuses = jsonResult.flightStatuses;
				if (!arrStatuses || !Array.isArray(arrStatuses)){
					notifications.notifyAdminAboutPostingProblem("no statuses: "+body);
					return device.sendMessageToDevice(from_address, 'text', "Bad data from flightstats.");
				}
				if (arrStatuses.length === 0)
					return device.sendMessageToDevice(from_address, 'text', "No information about this flight.");
				let lastFlightStatus = arrStatuses[arrStatuses.length-1];
				if (lastFlightStatus.status === 'S' || lastFlightStatus.status === 'A')
					return device.sendMessageToDevice(from_address, 'text', "The flight has not finished yet.");
				if (lastFlightStatus.status === 'U' || lastFlightStatus.status === 'DN')
					return device.sendMessageToDevice(from_address, 'text', "Flightstats doesn't know anything about this flight.");
				if (lastFlightStatus.status === 'NO')
					return device.sendMessageToDevice(from_address, 'text', "The flight is not operational.");
				if (lastFlightStatus.status !== 'L'){ // C, R, D
					var datafeed = {};
					datafeed[feed_name] = 10000;
					datafeed[feed_name+'-remark'] = status2text(lastFlightStatus.status);
					reliablyPostDataFeed(datafeed, from_address);
					return device.sendMessageToDevice(from_address, 'text', "The flight was canceled, diverted, or redirected.  This counts as large delay.\n\nThe data will be added into the database, I'll let you know when it is confirmed and you are able to unlock your contract.\n\n"+browser_url);
				}

				let operationalTimes = lastFlightStatus.operationalTimes;
				var plannedArrival, actualArrival;

				if (operationalTimes.publishedArrival)
					plannedArrival = Date.parse(operationalTimes.publishedArrival.dateUtc);
				else if (operationalTimes.scheduledGateArrival)
					plannedArrival = Date.parse(operationalTimes.scheduledGateArrival.dateUtc);
				else{
					notifications.notifyAdminAboutPostingProblem("no planned arrival for "+full_flight);
					return device.sendMessageToDevice(from_address, 'text', "Unable to determine planned arrival date.");
				}
				if (!plannedArrival || plannedArrival === NaN)
					throw Error("bad planned arrival date");

				if (operationalTimes.actualGateArrival)
					actualArrival = Date.parse(operationalTimes.actualGateArrival.dateUtc);
				else if (operationalTimes.estimatedGateArrival)
					actualArrival = Date.parse(operationalTimes.estimatedGateArrival.dateUtc);
				else if (operationalTimes.actualRunwayArrival){
					actualArrival = Date.parse(operationalTimes.actualRunwayArrival.dateUtc) + TAXI_IN_TIME;
					remark = 'runway';
				}
				else if (operationalTimes.estimatedRunwayArrival){
					actualArrival = Date.parse(operationalTimes.estimatedRunwayArrival.dateUtc) + TAXI_IN_TIME;
					remark = 'runway';
				}
				else{
					notifications.notifyAdminAboutPostingProblem("no planned arrival for "+full_flight);
					return device.sendMessageToDevice(from_address, 'text', "Unable to determine planned arrival date.");
				}
				if (!actualArrival || actualArrival === NaN || actualArrival <= TAXI_IN_TIME)
					throw Error("bad actual arrival date");

				var delay = Math.round((actualArrival - plannedArrival)/1000/60);
				var datafeed = {};
				datafeed[feed_name] = delay;
				if (remark)
					datafeed[feed_name+'-remark'] = remark;
				reliablyPostDataFeed(datafeed, from_address);
				device.sendMessageToDevice(from_address, 'text', getDelayText(delay, remark, false, browser_url));
			});
		});
	});

});

eventBus.on('my_transactions_became_stable', function(arrUnits){
	var device = require('byteballcore/device.js');
	db.query("SELECT feed_name FROM data_feeds WHERE unit IN(?)", [arrUnits], function(rows){
		rows.forEach(row => {
			let feed_name = row.feed_name;
			if (!assocDeviceAddressesByFeedName[feed_name])
				return;
			let arrDeviceAddresses = _.uniq(assocDeviceAddressesByFeedName[feed_name]);
			arrDeviceAddresses.forEach(device_address => {
				device.sendMessageToDevice(device_address, 'text', "The data about your flight "+feed_name+" is now in the database, you can unlock your contract.");
			});
			delete assocDeviceAddressesByFeedName[feed_name];
		});
	});
});


//////

eventBus.on('headless_wallet_ready', function(){
	if (!conf.admin_email || !conf.from_email){
		console.log("please specify admin_email and from_email in your "+desktopApp.getAppDataDir()+'/conf.json');
		process.exit(1);
	}
	if (!conf.flightstatsAppId || !conf.flightstatsAppKey){
		console.log("please specify flightstatsAppId and flightstatsAppKey in your "+desktopApp.getAppDataDir()+'/conf.json');
		process.exit(1);
	}
	headlessWallet.readSingleAddress(function(address){
		my_address = address;
	});
});
