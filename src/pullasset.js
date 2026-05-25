/**
 * 
 * 
node ./pullasset.js outFile=H:\current\query-profiles.json sourceApp=fusionapp sourceHost=fusionhost assetName=query-profiles source=query-profiles
node ./pullasset.js outFile=H:\current\query-pipelines.json sourceApp=fusionapp sourceHostfusionhost assetName=query-pipelines source=query-pipelines
node ./pullasset.js outFile=H:\current\index-profiles.json sourceApp=fusionapp sourceHost=fusionhost assetName=index-profiles source=index-profiles
node ./pullasset.js outFile=H:\currentt\index-pipelines.json sourceApp=fusionapp sourceHost=fusionhost assetName=index-pipelines source=index-pipelines
node ./pullasset.js outFile=H:\current\datasources.json sourceApp=fusionapp sourceHost=fusionhost assetName=connectors/datasources

* 
 */



const commandLine = {};

process.env.forEach((val, index) => {
  console.log(`${index}: ${val}`);
  if( index > 1 ){
	let v = val;
	
	if( v.indexOf("=") ){
		let name = v.substring(0,v.indexOf("="));
		commandLine[name] = v.substring(v.indexOf("=")+1);
		if( commandLine[name].endsWith("'") && commandLine[name].startsWith("'") )
			commandLine[name] = commandLine[name].substring(1,commandLine[name].length-2);
	}
  }
});

process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;

process.argv.forEach((val, index) => {
  console.log(`${index}: ${val}`);
  if( index > 1 ){
	let v = val;
	
	if( v.indexOf("=") ){
		let name = v.substring(0,v.indexOf("="));
		commandLine[name] = v.substring(v.indexOf("=")+1);
		if( commandLine[name].endsWith("'") && commandLine[name].startsWith("'") )
			commandLine[name] = commandLine[name].substring(1,commandLine[name].length-2);
	}
  }
});


console.log("commandline",commandLine);
var http = commandLine.http ? require('http') : require('https');
var fs = require('fs'),
	readline = require('readline'),
	stream = require('stream');
	
//process.exit(0);
var assetName = commandLine.hasOwnProperty('assetName') ? commandLine['assetName'] : 'query-profiles';
var sourceApp = commandLine.hasOwnProperty('sourceApp') ? commandLine['sourceApp'] : 'ParkerDotComMain';

var outFile = commandLine.hasOwnProperty('outFile') ? commandLine['outFile'] : 'asset.json';
var sourceHost = commandLine.hasOwnProperty('sourceHost') ? commandLine['sourceHost'] : "localhost";
var sourcePort = commandLine.hasOwnProperty('sourcePort') ? commandLine['sourcePort'] : 443;
var sourcePath = commandLine.hasOwnProperty('sourcePath') ? commandLine['sourcePath'] : "/api/" + (sourceApp ? 'apps/' + sourceApp + '/': '') + assetName;
var sourceKey = commandLine.hasOwnProperty('sourceKey') ? commandLine['sourceKey'] : "xxx";
var source = commandLine.hasOwnProperty('source') ? commandLine['source'] : assetName;
var defaultObjects = commandLine.hasOwnProperty('defaultObjects') ? JSON.parse(commandLine['defaultObjects']) : {};

var includeOnly = commandLine.hasOwnProperty('includeOnly') ? commandLine['includeOnly'] : "";

var debug = commandLine.hasOwnProperty('debug') ? parseInt(commandLine['debug']): 11;

var allData = {};

function pullCallback(res) {
  var str = "";
  res.source = this.source;
  
  res.on('data', function (chunk) {
              str += chunk;
               
        });

  res.on('end', function () {
		let handlerSource = res.source;
		if( debug > 2 ) console.log("pull",str);
		let data = JSON.parse(str);
		
		allData[handlerSource] = data;
		
		doPull(handlerSource,data);
  });
}

function doPull(handlerSource,sourceData){	
	if( handlers.hasOwnProperty(handlerSource) ){
		handlers[handlerSource](handlerSource,sourceData);
	}
	else {
		if( debug > 0 ) console.log("no source handler for ",handlerSource);
	}
}

function pullAsset(){
	let tCallback = pullCallback.bind({source: source});
	
	let t = http.request({hostname: sourceHost,port: sourcePort,path: sourcePath,method: 'GET',
      rejectUnauthorized: false,headers: {'Authorization': "Basic " + sourceKey,'Content-Type': 'application/json'}}, tCallback);
	t.on('error', function(e) {if( debug > 0 ) console.log("Got error: " + e.message);});
	t.end();
}

pullAsset();


var handlers = {};

handlers["query-profiles"] = function(handlerSource,frame){
		console.log("update query profile",includeOnly);
		
		if( frame ){
			let newFrame = [];
			for(let p in frame){
			    let oneFromSource = frame[p];
				if( includeOnly.length == 0 || includeOnly == oneFromSource.id ){
					newFrame.push(oneFromSource);
				}
			}
			fs.writeFile(outFile,JSON.stringify(newFrame,null,5),function (err) {
			if (err) return console.log(err);
			console.log(outFile);
			});
		}
}

handlers["query-pipelines"] = function(handlerSource,frame){
		console.log("update query pipeline",includeOnly);
	if( frame ){
			let newFrame = [];
			for(let p in frame){
			    let oneFromSource = frame[p];
				if( includeOnly.length == 0 || includeOnly == oneFromSource.id ){
					newFrame.push(oneFromSource);
				}
			}
			fs.writeFile(outFile,JSON.stringify(newFrame,null,5),function (err) {
			if (err) return console.log(err);
			console.log(outFile);
			});
		}
}

handlers["index-profiles"] = function(handlerSource,frame){
	console.log("update index profiles",includeOnly);
	if( frame ){
			let newFrame = [];
			for(let p in frame){
			    let oneFromSource = frame[p];
				if( includeOnly.length == 0 || includeOnly == oneFromSource.id ){
					newFrame.push(oneFromSource);
				}
			}
			fs.writeFile(outFile,JSON.stringify(newFrame,null,5),function (err) {
			if (err) return console.log(err);
			console.log(outFile);
			});
		}
}

handlers["index-pipelines"] = function(handlerSource,frame){
	console.log("update index pipeline",includeOnly);
	if( frame ){
			let newFrame = [];
			for(let p in frame){
			    let oneFromSource = frame[p];
				if( includeOnly.length == 0 || includeOnly == oneFromSource.id ){
					newFrame.push(oneFromSource);
				}
			}
			fs.writeFile(outFile,JSON.stringify(newFrame,null,5),function (err) {
			if (err) return console.log(err);
			console.log(outFile);
			});
		}
}

handlers["parsers"] = function(handlerSource,frame){
	console.log("update parsers",includeOnly);
	if( frame ){
			let newFrame = [];
			for(let p in frame){
			    let oneFromSource = frame[p];
				if( includeOnly.length == 0 || includeOnly == oneFromSource.id ){
					newFrame.push(oneFromSource);
				}
			}
			fs.writeFile(outFile,JSON.stringify(newFrame,null,5),function (err) {
			if (err) return console.log(err);
			console.log(outFile);
			});
		}
}

handlers["connectors/datasources"] = function(handlerSource,frame){
	console.log("update datasources",includeOnly);
	if( frame ){
			let newFrame = [];
			for(let p in frame){
			    let oneFromSource = frame[p];
				if( includeOnly.length == 0 || includeOnly == oneFromSource.id ){
					newFrame.push(oneFromSource);
				}
			}
			fs.writeFile(outFile,JSON.stringify(newFrame,null,5),function (err) {
			if (err) return console.log(err);
			console.log(outFile);
			});
		}
}




