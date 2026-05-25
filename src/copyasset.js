/**
 * datasources
 * 
 * 

node ./copyasset.js sourceFile=H:/prod_assets/prod_assets/index-pipelines.json targetKey=xxx targetApp=fusionapp targetHost=fusionhost targetPort=443 assetName=index-pipelines source=index-pipelines defaultObjects="{}"
node ./copyasset.js sourceFile=H:/prod_assets/prod_assets/index-profiles.json targetKey=xxx targetApp=fusionapp targetHost=fusionhost targetPort=443 assetName=index-profiles source=index-profiles defaultObjects="{}"
node ./copyasset.js sourceFile=H:/prod_assets/prod_assets/datasources.json targetKey=xxx targetApp=fusionapp targetHost=fusionhost targetPort=443 assetName=connectors/datasources source=datasources defaultObjects="{}"
node ./copyasset.js sourceFile=H:/prod_assets/prod_assets/query-profiles.json targetKey=xxx targetApp=fusionapp targetHost=fusionhost targetPort=443 assetName=query-profiles source=query-profiles defaultObjects="{}"
node ./copyasset.js sourceFile=H:/prod_assets/prod_assets/query-pipelines.json targetKey=xxx targetApp=fusionapp targetHost=fusionhost targetPort=443 assetName=query-pipelines source=query-pipelines defaultObjects="{}"
* 
 */

const https = require('https');
const fs = require('fs'),
	readline = require('readline'),
	stream = require('stream');

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

process.env["NODE_TLS_REJECT_UNAUTHORIZED"]=0;

console.log("commandline",commandLine);

//process.exit(0);
const assetName = commandLine.hasOwnProperty('assetName') ? commandLine['assetName'] : 'query-profiles';
const sourceApp = commandLine.hasOwnProperty('sourceApp') ? commandLine['sourceApp'] : '';
const targetApp = commandLine.hasOwnProperty('targetApp') ? commandLine['targetApp'] : '';

const sourceFile = commandLine.hasOwnProperty('sourceFile') ? commandLine['sourceFile'] : false;
const sourceHost = commandLine.hasOwnProperty('sourceHost') ? commandLine['sourceHost'] : "fusdionhost";
const sourcePort = commandLine.hasOwnProperty('sourcePort') ? commandLine['sourcePort'] : 443;
const sourcePath = commandLine.hasOwnProperty('sourcePath') ? commandLine['sourcePath'] : "/api/" + (sourceApp ? 'apps/' + sourceApp + '/': '') + assetName;
const sourceKey = commandLine.hasOwnProperty('sourceKey') ? commandLine['sourceKey'] : "";
const source = commandLine.hasOwnProperty('source') ? commandLine['source'] : assetName;
const defaultObjects = commandLine.hasOwnProperty('defaultObjects') ? JSON.parse(commandLine['defaultObjects']) : {};

const excludeList = commandLine.hasOwnProperty('excludeList') ? commandLine['excludeList'].split(",") : [];
const includeOnly = commandLine.hasOwnProperty('includeOnly') ? commandLine['includeOnly'] : "";
const targetHost = commandLine.hasOwnProperty('targetHost') ? commandLine['targetHost'] : "fusionhost";
const targetPort = commandLine.hasOwnProperty('targetPort') ? commandLine['targetPort'] : 443;
const targetKey = commandLine.hasOwnProperty('targetKey') ? commandLine['targetKey'] : "";
const targetAssetName = commandLine.hasOwnProperty('targetAssetName') ? commandLine['targetAssetName'] : assetName;
const targetPath = commandLine.hasOwnProperty('targetPath') ? commandLine['targetPath'] : "/api/" + (targetApp ? 'apps/' + targetApp + '/': '') +  targetAssetName;
const targetSuffix = commandLine.hasOwnProperty('targetSuffix') ? commandLine['targetSuffix'] : false;
const assetKey = commandLine.hasOwnProperty('assetKey') ? commandLine['assetKey'] : 'id';
const deleteOnly = commandLine.hasOwnProperty('deleteOnly') ? commandLine['deleteOnly'] === 'true' : false;
const debug = commandLine.hasOwnProperty('debug') ? parseInt(commandLine['debug']): 3;

const allData = {};

function convertCallback(res) {
  let str = "";
  res.source = this.source;
  
  res.on('data', function (chunk) {
              str += chunk;
               
        });

  res.on('end', function () {
		let handlerSource = res.source;
		let data = JSON.parse(str);
		
		allData[handlerSource] = data;
		
		doConversion(handlerSource,data);
  });
}

function doConversion(handlerSource,sourceData){	
	if( handlers.hasOwnProperty(handlerSource) ){
		handlers[handlerSource](handlerSource,sourceData);
	}
	else {
		if( debug > 0 ) console.log("no source handler for ",handlerSource);
	}
}

function updateCallback(res){
  let str = "";
  let asset = this.asset;
  let assetName = this.assetName;
  let source = this.source;
  let callback = this.callback;
  let statusCode = res.statusCode;
  
  res.on('data', function (chunk) {
              str += chunk;
               
        });

  res.on('end', function () {
	//if( debug > 0 ) console.log(str);
	if( asset.id == "PDCM-Parts-G1-JDBC" ){
		console.log(asset);
	}
	if( debug > 1 ) console.log("updatecb ",statusCode,source,assetName,asset,str);
	if( callback ) callback(source,assetName,str);
  });
}

function deleteCallback(res){
	let str = "";
	let asset = this.asset;
	let source = this.source;
	let callback = this.callback;
		
	res.on('data', function (chunk) {
				str += chunk;
				 
		  });
  
	res.on('end', function () {
	  //if( debug > 0 ) console.log(str);
	  if( asset.id == "PDCM-Parts-G1-JDBC" ){
		console.log(asset);
	}
	  if( debug > 1 ) console.log("delete ",source,asset,str);

	  if( !deleteOnly ){
		  //callback(source,assetName,str);
		  setTimeout(callback,2000,source,assetName,str);
	  }
	});
  }

function deleteAsset(handlerSource,asset,callback){
	let tCallback = deleteCallback.bind({source: handlerSource,asset: asset,callback: callback});
	let tPath = targetPath + "/" + asset[assetKey] + (targetSuffix ? "/" + targetSuffix : '');
	
	let t = https.request({hostname: targetHost,port: targetPort,path: tPath,method: 'DELETE',headers: {'Authorization': "Basic " + targetKey,'Content-Type': 'application/json'}}, tCallback);
	t.on('error', function(e) {if( debug > 0 ) console.log("Got error: " + e.message);});
	t.write(JSON.stringify(asset));
	t.end();
}

function updateAsset(handlerSource,asset,cb){
	let tCallback = updateCallback.bind({source: handlerSource,asset: asset,callback: cb});
	
	let t = https.request({hostname: targetHost,port: targetPort,path: targetPath,method: 'POST',headers: {'Authorization': "Basic " + targetKey,'Content-Type': 'application/json'}}, tCallback);
	
	t.on('socket', function (socket) {
				socket.setTimeout(300000);  
				socket.on('timeout', function() {
						t.abort();
					});
				});
	
	t.on('error', function(e) {if( debug > 0 ) console.log("Got error: " + e.message);});
	t.write(JSON.stringify(asset));
	t.end();
}

function convertAsset(){
	let tCallback = convertCallback.bind({source: source});
	
	let t = https.request({hostname: sourceHost,port: sourcePort,path: sourcePath,method: 'GET',headers: {'Authorization': "Basic " + sourceKey,'Content-Type': 'application/json'}}, tCallback);
	t.on('error', function(e) {if( debug > 0 ) console.log("Got error: " + e.message);});
	t.end();
}

if( sourceFile ){
	const instream = fs.createReadStream(sourceFile);
	instream.readable = true;

	const rl = readline.createInterface({
		input: instream,
		terminal: false
	});
	var fileData = '';
	function readFunc(line) {
		if( debug > 3 ) console.log(line);
	    fileData += line;
	}

	function doComplete(){
		if( debug > 0 ) console.log("complete");
		let sourceData = JSON.parse(fileData);
	   doConversion(source,sourceData);
	}
	rl.on('line', readFunc);
	rl.on('close', doComplete);

}
else
	convertAsset();


var handlers = {};

handlers["schedules"] = function(handlerSource,frame){
	console.log("update job schedule",includeOnly);
var counter = 0;
let cb = function(){
	let newRec = false;
	
	for(let slot in frame){
		let oneFromSource = frame[slot];
		if( (includeOnly.length == 0 || includeOnly == oneFromSource.id) && excludeList.indexOf(oneFromSource.id) < 0 ){
			counter++;
		
			if( debug > 0 ) console.log("doing",oneFromSource.resource,counter);
			newRec = {};
			
			for(let a in oneFromSource){
				if( oneFromSource.hasOwnProperty(a) ){
					newRec[a] = oneFromSource[a];
				}
			}
			
			if( debug > 0 ) console.log("newrec",newRec.resource);
			if( newRec ){
				let tc = function(){
					if( debug > 0 ) console.log("newrec schedule update",this.newRec.resource);
					let tCallback = updateCallback.bind({source: handlerSource,assetName: this.newRec.resource,asset: this.newRec});
					let tPath = targetPath + "/" + this.newRec[assetKey] + (targetSuffix ? "/" + targetSuffix : '');
	
					let t = https.request({hostname: targetHost,port: targetPort,path: tPath,method: 'PUT',headers: {'Authorization': "Basic " + targetKey,'Content-Type': 'application/json'}}, tCallback);
					t.on('error', function(e) {if( debug > 0 ) console.log("Got error: " + e.message);});
					t.write(JSON.stringify(this.newRec));
					t.end();
				}
				if( debug > 0 ) console.log("newrec delete",newRec.resource);
				let ttc = tc.bind({handlerSource: handlerSource,newRec: newRec});
				deleteAsset(handlerSource,newRec,ttc);
			}
		}
		else {
			if( debug > 0 ) console.log("skipping",oneFromSource.resource,counter);
		}
		
		//delete frame[slot];
	}
	
	if( frame.length == 0 ){
		if( debug > 0 ) console.log("copy complete",counter);
	}
}

cb();
}

handlers["tasks"] = function(handlerSource,frame){
	console.log("update task job",includeOnly);
var counter = 0;
let cb = function(){
	let newRec = false;
	
	for(let slot in frame){
		let oneFromSource = frame[slot];
		if( (includeOnly.length == 0 || includeOnly == oneFromSource.id) && excludeList.indexOf(oneFromSource.id) < 0 ){
			counter++;
		
			if( debug > 0 ) console.log("doing",oneFromSource.id,counter);
			newRec = {};
			
			for(let a in oneFromSource){
				if( oneFromSource.hasOwnProperty(a) ){
					newRec[a] = oneFromSource[a];
				}
			}
			
			if( debug > 0 ) console.log("newrec",newRec.id);
			if( newRec ){
				let tc = function(){
					if( debug > 0 ) console.log("newrec update",this.newRec.id);
				
					updateAsset(this.handlerSource,this.newRec,cb);
				}
				if( debug > 0 ) console.log("newrec delete",newRec.id);
				let ttc = tc.bind({handlerSource: handlerSource,newRec: newRec});
				deleteAsset(handlerSource,newRec,ttc);
			}
		}
		else {
			if( debug > 0 ) console.log("skipping",oneFromSource.id,counter);
		}
		
		delete frame[slot];
	}
	
	if( frame.length == 0 ){
		if( debug > 0 ) console.log("copy complete",counter);
	}
}

cb();
}

handlers["spark/configurations"] = function(handlerSource,frame){
	console.log("update spark job",includeOnly);
var counter = 0;
let cb = function(){
	let newRec = false;
	
	for(let slot in frame){
		let oneFromSource = frame[slot];
		if( (includeOnly.length == 0 || includeOnly == oneFromSource.id) && excludeList.indexOf(oneFromSource.id) < 0 ){
			counter++;
		
			if( debug > 0 ) console.log("doing",oneFromSource.id,counter);
			newRec = {};
			
			for(let a in oneFromSource){
				if( oneFromSource.hasOwnProperty(a) ){
					newRec[a] = oneFromSource[a];
				}
			}
			
			if( debug > 0 ) console.log("newrec",newRec.id);
			if( newRec ){
				let tc = function(){
					if( debug > 0 ) console.log("newrec update",this.newRec.id);
				
					updateAsset(this.handlerSource,this.newRec,cb);
				}
				if( debug > 0 ) console.log("newrec delete",newRec.id);
				let ttc = tc.bind({handlerSource: handlerSource,newRec: newRec});
				deleteAsset(handlerSource,newRec,ttc);
			}
		}
		else {
			if( debug > 0 ) console.log("skipping",oneFromSource.id,counter);
		}
		
		delete frame[slot];
	}
	
	if( frame.length == 0 ){
		if( debug > 0 ) console.log("copy complete",counter);
	}
}

cb();
}

handlers["query-profiles"] = function(handlerSource,frame){
		console.log("update query profile",includeOnly);
	var counter = 0;
	let cb = function(){
		let newRec = false;
		
		for(let slot in frame){
			let oneFromSource = frame[slot];
			if( (includeOnly.length == 0 || includeOnly == oneFromSource.id) && excludeList.indexOf(oneFromSource.id) < 0 ){
				counter++;
			
				if( debug > 0 ) console.log("doing",oneFromSource.id,counter);
				newRec = {};
				
				for(let a in oneFromSource){
					if( oneFromSource.hasOwnProperty(a) ){
						newRec[a] = oneFromSource[a];
					}
				}
				
				if( debug > 0 ) console.log("newrec",newRec.id);
				if( newRec ){
					let tc = function(){
						if( debug > 0 ) console.log("newrec update",this.newRec.id);
					
						updateAsset(this.handlerSource,this.newRec,cb);
					}
					if( debug > 0 ) console.log("newrec delete",newRec.id);
					let ttc = tc.bind({handlerSource: handlerSource,newRec: newRec});
					deleteAsset(handlerSource,newRec,ttc);
				}
			}
			else {
				if( debug > 0 ) console.log("skipping",oneFromSource.id,counter);
			}
			
			delete frame[slot];
		}
		
		if( frame.length == 0 ){
			if( debug > 0 ) console.log("copy complete",counter);
		}
	}

	cb();
}

handlers["query-pipelines"] = function(handlerSource,frame){
		console.log("update query pipeline",includeOnly);
	var counter = 0;
	let cb = function(){
		let newRec = false;
		
		let ucb = function(res){
			console.log(res);
			asyncLoop(this.ctx);
		};
		
		let loopCtx = {docs: frame,offset: 0,callback: cb};
		let asyncLoop = function(ctx){
			if( ctx.offset < ctx.docs.length ){
				if( debug > 0 ) console.log("doing",ctx.docs[ctx.offset].id,ctx.offset);
				let oneFromSource = ctx.docs[ctx.offset++];
				if( (includeOnly.length == 0 || includeOnly == oneFromSource.id) && excludeList.indexOf(oneFromSource.id) < 0 ){
					newRec = {};
					
					for(let a in oneFromSource){
						if( oneFromSource.hasOwnProperty(a) ){
							newRec[a] = oneFromSource[a];
						}
					}
					
					if( debug > 0 ) console.log("newrec",newRec.id);
					if( newRec ){
						let tc = function(){
							if( debug > 0 ) console.log("newrec update",this.newRec.id);
						
							updateAsset(this.handlerSource,this.newRec,ucb.bind({handlerSource: this.handlerSource,newRec: this.newRec,ctx: this.ctx}));
						}
						if( debug > 0 ) console.log("newrec delete",newRec.id);
						let ttc = tc.bind({ctx,handlerSource,newRec});
						deleteAsset(handlerSource,newRec,ttc);
					}
				}
				else {
					if( debug > 0 ) console.log("skipping",oneFromSource.id);
				}
				
				//delete frame[slot];
			}
			else {
					console.log("done");
			}
			
		}
		
		asyncLoop(loopCtx);
	}

	cb();
}

handlers["index-profiles"] = function(handlerSource,frame){
	console.log("update index profiles",includeOnly);
	var counter = 0;
	let cb = function(){
		let newRec = false;
		
		for(let slot in frame){
			let oneFromSource = frame[slot];
			if( (includeOnly.length == 0 || includeOnly == oneFromSource.id) && excludeList.indexOf(oneFromSource.id) < 0 ){
				counter++;
			
				if( debug > 0 ) console.log("doing",oneFromSource.id,counter);
				newRec = {};
				
				for(let a in oneFromSource){
					if( oneFromSource.hasOwnProperty(a) ){
						newRec[a] = oneFromSource[a];
					}
				}
				
				if( debug > 0 ) console.log("newrec",newRec.id);
				if( newRec ){
					let tc = function(){
						if( debug > 0 ) console.log("newrec update",this.newRec.id);
					
						updateAsset(this.handlerSource,this.newRec,cb);
					}
					if( debug > 0 ) console.log("newrec delete",newRec.id);
					let ttc = tc.bind({handlerSource: handlerSource,newRec: newRec});
					deleteAsset(handlerSource,newRec,ttc);
				}
			}
			else {
				if( debug > 0 ) console.log("skipping",oneFromSource.id,counter);
			}
			
			delete frame[slot];
		}
		
		if( frame.length == 0 ){
			if( debug > 0 ) console.log("copy complete",counter);
		}
	}

	cb();
}

handlers["index-pipelines"] = function(handlerSource,frame){
	console.log("update index pipeline",includeOnly);
	var counter = 0;
	let cb = function(){
		let newRec = false;
		
		for(let slot in frame){
			let oneFromSource = frame[slot];
			if( (includeOnly.length == 0 || includeOnly == oneFromSource.id) && excludeList.indexOf(oneFromSource.id) < 0 ){
				counter++;
			
				if( debug > 0 ) console.log("doing",oneFromSource.id,counter);
				newRec = {};
				
				for(let a in oneFromSource){
					if( oneFromSource.hasOwnProperty(a) ){
						newRec[a] = oneFromSource[a];
					}
				}
				
				/*for(let stage of newRec.stages){
					if( stage.type == "managed-js-index" ){
						stage.skip = true;
					}
				}*/
				
				
				if( debug > 0 ) console.log("newrec",newRec.id);
				if( newRec ){
					let tc = function(){
						if( debug > 0 ) console.log("newrec update",this.newRec.id);
					
						updateAsset(this.handlerSource,this.newRec,cb);
					}
					if( debug > 0 ) console.log("newrec delete",newRec.id);
					let ttc = tc.bind({handlerSource: handlerSource,newRec: newRec});
					deleteAsset(handlerSource,newRec,ttc);
				}
			}
			else {
				if( debug > 0 ) console.log("skipping",oneFromSource.id,counter);
			}
			
			delete frame[slot];
		}
		
		if( frame.length == 0 ){
			if( debug > 0 ) console.log("copy complete",counter);
		}
	}

	cb();
}

handlers["parsers"] = function(handlerSource,frame){
	console.log("update parsers",includeOnly);
	var counter = 0;
	let cb = function(){
		let newRec = false;
		
		for(let slot in frame){
			let oneFromSource = frame[slot];
			if( (includeOnly.length == 0 || includeOnly == oneFromSource.id) && excludeList.indexOf(oneFromSource.id) < 0 ){
				counter++;
			
				if( debug > 0 ) console.log("doing",oneFromSource.id,counter);
				newRec = {};
				
				for(let a in oneFromSource){
					if( oneFromSource.hasOwnProperty(a) ){
						newRec[a] = oneFromSource[a];
					}
				}
				
				if( debug > 0 ) console.log("newrec",newRec.id);
				if( newRec ){
					let tc = function(){
						if( debug > 0 ) console.log("newrec update",this.newRec.id);
					
						updateAsset(this.handlerSource,this.newRec,cb);
					}
					if( debug > 0 ) console.log("newrec delete",newRec.id);
					let ttc = tc.bind({handlerSource: handlerSource,newRec: newRec});
					deleteAsset(handlerSource,newRec,ttc);
				}
			}
			else {
				if( debug > 0 ) console.log("skipping",oneFromSource.id,counter);
			}
			
			delete frame[slot];
		}
		
		if( frame.length == 0 ){
			if( debug > 0 ) console.log("copy complete",counter);
		}
	}

	cb();
}

handlers["datasources"] = function(handlerSource,frame){
	console.log("update datasources",includeOnly);
	var counter = 0;
	let cb = function(){
		let newRec = false;
		
		for(let slot in frame){
			let oneFromSource = frame[slot];
			if( (includeOnly.length == 0 || includeOnly == oneFromSource.id) && excludeList.indexOf(oneFromSource.id) < 0 ){
				counter++;
			
				if( debug > 0 ) console.log("doing",oneFromSource.id,counter);
				newRec = {};
				
				for(let a in oneFromSource){
					if( oneFromSource.hasOwnProperty(a) ){
						newRec[a] = defaultObjects[a] ? defaultObjects[a] : oneFromSource[a];
					}
				}
				if( debug > 0 ) console.log("newrec type",newRec.type);
				if( newRec.type == 'jdbc' && defaultObjects ){
					newRec.type = "lucidworks.jdbc";
					newRec.connector = "lucidworks.jdbc";
					delete newRec["initial-mapping"];
					newRec.parserId = "_system";
					
					newRec.properties.disableAutomaticPagination = true;
					newRec.properties.enableAutomaticPagination = false;
					
					newRec.properties.query = newRec.properties.sql_select_statement.replaceAll("\n"," ");
					
					if( newRec.id == "PDCM-Categories-JDBC" ){
							newRec.properties.query = newRec.properties.query.replace("*","id,categoryId_l,parentId_l,language_is,languageName_ss,title_t,isL1_s,isL2_s,isL3_s,isL4_s,description_t,searchableDescription_t,body_t,thumbnailURL_s,imageURL_s,parkerDivisionName_ss,parkerDivision_ss,parkerOwningDivision_ss,parkerOwningDivisionName_ss,hasCad_s,hasConfig_s,urlKeyword_s,singleUrlKeyword_s,keywordGrouping_s,countryId_ss,countryName_ss,contentSource_s,siteSection_ss,siteSubSection_ss,productType_ss,productType_s,audience_ss,productTaxonomyL1_s,productTaxonomyL2_s,productTaxonomyL3_s,productTaxonomyL4_s,productTaxonomy_t,productTaxonomy_ss,countryCode_ss,storeId_l,FORMAT(lastModifiedDate_dt, 'yyyy-MM-ddTHH:mm:ss.fffZ') as lastModifiedDate_dt");
					}
					
					if( newRec.description && newRec.description.length > 125 ){
						newRec.description = newRec.description.substring(newRec.description.length - 125);	
					}
					
					delete newRec.properties["sql_select_statement"];
					
					if( defaultObjects.password ){
						newRec.properties.password = defaultObjects.password;
						if( debug > 0 ) console.log("newrec set password");
					}
					
					if( defaultObjects.url ) {
						newRec.properties.url = defaultObjects.url;
						if( debug > 0 ) console.log("newrec set url");
					}
					
					newRec.properties.verify_access = false;
				}
				else {
					for(let a in oneFromSource.properties){
						if( oneFromSource.properties.hasOwnProperty(a) ){
							newRec.properties[a] = defaultObjects[a] ? defaultObjects[a] : oneFromSource.properties[a];
						}
					}
					
					newRec.properties.verify_access = false;
				}
				if( debug > 0 ) console.log("newrec",newRec.id);
				if( newRec ){
					let tc = function(){
						if( debug > 0 ) console.log("newrec update",this.newRec.id);
					
						updateAsset(this.handlerSource,this.newRec,cb);
					}
					if( debug > 0 ) console.log("newrec delete",newRec.id);
					let ttc = tc.bind({handlerSource: handlerSource,newRec: newRec});
					deleteAsset(handlerSource,newRec,ttc);
				}
			}
			else {
				if( debug > 0 ) console.log("skipping",oneFromSource.id,counter);
			}
			
			delete frame[slot];
		}
		
		if( frame.length == 0 ){
			if( debug > 0 ) console.log("copy complete",counter);
		}
	}

	cb();
}




