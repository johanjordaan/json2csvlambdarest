const StartFile = (name) => {
  return {
    name:name,
    headers:[],
    csvlines:[],
    children:[]
  };
};

const AddLine = (context, line, processHeaders) => {
  var json = JSON.parse(line);
  var keys = Object.keys(json);
  
  if(processHeaders === true) {
    keys.forEach((key)=>{
      if(!context.headers.includes(key)) {
        context.headers.push(key);
      }
    });
  }
  
  var values = context.headers.map((header)=>{
    if(keys.includes(header)) {
      return json[header].toString();
    } else {
      return "";
    }
  });
  
  context.csvlines.push(values.join(",")+"\n");
  
  return context;
};

const AddLines = (context, data) => {
  const lines = data.split("\n");
  
  for(var i=0;i<lines.length;i++) {
    context = AddLine(context,lines[i],i==0);
  }
  
  return context;
}


module.exports = {
  StartFile,
  AddLine,
  AddLines,
};