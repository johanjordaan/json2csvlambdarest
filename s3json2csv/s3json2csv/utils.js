const isJson = (str) => {
  try {
    JSON.parse(str);
    return true;
  } catch(err) {
    console.log(err);
    return false;
  }
};

const fail = (code,message) => {
  return {
    isBase64Encoded:false,
    statusCode: code,
    headers: {},
    body:JSON.stringify(message,null,2),
  };
};

const succeed = (message,origin,methods) => {
  return {
    isBase64Encoded:false,
    statusCode: 200,
    headers: {
        "Access-Control-Allow-Headers" : "*",
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Methods": methods
    },
    body:JSON.stringify(message,null,2),
  };
};

const cors = (origin,methods) => {
  return {
    isBase64Encoded:false,
    statusCode: 200,
    headers: {
        "Access-Control-Allow-Headers" : "*",
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Methods": methods
    },
    body:""
  };
};


module.exports = {
  isJson,
  fail,
  succeed,
  cors
};