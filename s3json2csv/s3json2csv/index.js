const utils = require('./utils');
const json2csv = require('./json2csv');
const AWS = require('aws-sdk');
const S3 = new AWS.S3();


exports.handler =  async function(event, context) {
    
  if(event.httpMethod === "OPTIONS") {
    return utils.cors('*','OPTIONS,POST');
  }  
    
  if(event.httpMethod !== "POST") {
    return utils.fail(404,{message: `${event.httpMethod} not supported`});
  }
  
  if(event.queryStringParameters === null || event.queryStringParameters.appName === null) {
    return utils.fail(404,{message: `appName queryStringParameters not provided`});
  }
  const appName = event.queryStringParameters.appName;

  if(event.queryStringParameters === null || event.queryStringParameters.sectionName === null) {
    return utils.fail(404,{message: `sectionName queryStringParameters not provided`});
  }
  var sectionName = event.queryStringParameters.sectionName;;

  var appLookup = {};
  try {
    const data = await S3.getObject({
      Bucket: "1337coders-config",
      Key:"s3json2csv/config.json"
    }).promise();
    const config = JSON.parse(data.Body.toString('ascii'));
    appLookup = config.applications.reduceRight((a,i)=>{
      a[i.name] = i;
      return a;
    },{});
  } catch(err) {
    console.log(err)
    return utils.fail(500,{message: `error loading config [${JSON.stringify(err)}]`});
  } 

  const app = appLookup[appName];
  if(app === null || app === undefined) {
    return utils.fail(404,{messsage: `app [${appName}] not found`});
  }

  if(app.sections === null || app.sections === undefined || !app.sections.includes(sectionName)) {
    return utils.fail(404,{messsage: `section [${sectionName}] not found`});
  }


  if(event.queryStringParameters === null || event.queryStringParameters.year === undefined) {
    return utils.fail(404,{message: `year queryStringParameters not provided`});
  }
  var year = event.queryStringParameters.year;

  if(event.queryStringParameters === null || event.queryStringParameters.month === undefined) {
    return utils.fail(404,{message: `month queryStringParameters not provided`});
  }
  var month = event.queryStringParameters.month;


  var day = 24;
  const inputprefix = `${app.inputfolder}/${sectionName}/year=${year}/month=${month}/day=${day}`;
  
  try {
    var data = await S3.listObjectsV2({
      Bucket: app.bucket,
      Prefix: inputprefix,
    }).promise();

    var csvContext = json2csv.StartFile(sectionName);
    console.log(data.Contents.length);

    for(const i in data.Contents) {
      console.log(data.Contents[i].Key);
      var fileData = await S3.getObject({
        Bucket: app.bucket,
        Key:data.Contents[i].Key
      }).promise();
      const fileDataStr = fileData.Body.toString('ascii');
      csvContext = json2csv.AddLines(csvContext,fileDataStr);
    }

    console.log(csvContext.csvlines);

    

    while(data.IsTruncated) {
      data = await S3.listObjectsV2({
        Bucket: app.bucket,
        ContinuationToken: data.NextContinuationToken,
      }).promise();

      if(data.Contents.length>1) {
        console.log(data.Contents[0].Key);
      }
    }


    return utils.succeed({message: `HALLO`},'*','OPTIONS,POST');
  } catch(err) {
    return utils.fail(500,{message: `error saving file [${JSON.stringify(err)}]`});
  }
};