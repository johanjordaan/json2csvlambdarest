const AWS = require('aws-sdk');
const S3 = new AWS.S3();

const StartFile = async (bucket,prefix,section,year,month,day) => {
  var key = `${prefix}/${section}_${year}${month}${day}.csv`;
  var mpu = await S3.createMultipartUpload({
    Bucket:bucket,
    Key: key
  }).promise();

  return {
    bucket:bucket,
    prefix:prefix,
    key:key,
    section:section,
    year:year,
    month:month,
    day:day,
    headers:[],
    csvlines:[],
    children:{},
    mpu:mpu,
    parts:[],
  };
};

const Upload = async (context) => {
  if(context.csvlines.length === 0) {
    return context;
  }
  console.log("uploading",context.key);

  var textToUpload = "";
  if(context.parts.length === 0) {
    textToUpload = context.headers.join(',')+'\n';
  }
  textToUpload = textToUpload + context.csvlines.join("\n");
  
  var part = await S3.uploadPart({
    Bucket:context.bucket,
    Key:context.key,
    UploadId:context.mpu.UploadId,
    PartNumber:context.parts.length+1,
    Body: textToUpload,
  }).promise();
  context.parts.push({
    ETag: part.ETag,
    PartNumber:context.parts.length+1
  });
  context.csvlines = [];
  
  for(var key in context.children) {
    await Upload(context.children[key]); 
  }
  
  return context;
};

const Complete = async (context) => {
  await Upload(context);
  await S3.completeMultipartUpload({
    Bucket:context.bucket,
    Key:context.key,
    UploadId: context.mpu.UploadId,
    MultipartUpload:{
      Parts: context.parts
    }
  }).promise();
  context.parts = [];

  for(var key in context.children) {
    await Complete(context.children[key]); 
  }

  return context;
};

const AddLine = async (context, data, processHeaders, parentId) => {
  var json = data;
  if (typeof data === 'string' || data instanceof String) {
    json = JSON.parse(data);
  }
  
  if(parentId !== undefined) {
    json.parentId = parentId; 
  }
  
  var keys = Object.keys(json);
  
  if(processHeaders === true) {
    keys.forEach((key)=>{
      if(!context.headers.includes(key)) {
        context.headers.push(key);
      }
    });
  }
  
  var values = await await Promise.all(context.headers.map(async (header)=>{
    if(keys.includes(header)) {
      var value = json[header];
      if(value===null || value===undefined) {
        return "";
      } else if(Array.isArray(value)) {
        if(value.length===0) return "";
        if(context.children[header] === undefined) {
          context.children[header] = await StartFile(context.bucket,context.prefix,`${context.section}_${header}`,context.year,context.month,context.day);
        }
        context.children[header] = await AddLines(context.children[header],value,json.Id);
        return `<<external->${context.section}_${header}>>`;
      } else if(typeof value === "object") {
        if(context.children[header] === undefined) {
          context.children[header] = await StartFile(context.bucket,context.prefix,`${context.section}_${header}`,context.year,context.month,context.day);
        }
        context.children[header] = await AddLine(context.children[header],value,true, json.Id);
        return `<<external->${context.section}_${header}>>`;
      } else {
        return value.toString();
      }
    } else {
      return "";
    }
  }));
  
  context.csvlines.push(values.join(","));
  
  return context;
};

const AddLines = async (context, data, parentId) => {
  var items = data;
  if (typeof data === 'string' || data instanceof String) {
    items = data.split("\n");
  }

  for(var i=0;i<items.length;i++) {
    context = await AddLine(context,items[i],i==0,parentId);
  }
  
  return context;
};


module.exports = {
  StartFile,
  Upload,
  Complete,
  AddLine,
  AddLines,
};