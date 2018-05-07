/**
 * Default language
 */
var DEFAULT_LANGUAGE = 'en';

/**
 * Schema
 */
var DataSchema = [
  {
    name: 'id_hash',
    label: { 
      pl: 'Id', 
      en: 'Id' 
    },
    dataType: 'STRING',
    semantics: {
      conceptType: 'DIMENSION' 
    }
  },
  { 
    name: 'sent', 
    label: { 
      pl: 'Wysłane', 
      en: 'Sent'
    },
    dataType: 'STRING',
    semantics: {
      contentType: 'DIMENSION',
      semanticGroup: 'DATETIME'
    }
  },
  {
    name: 'topic',
    label: {
      en: 'Topic',
      pl: 'Temat' 
    },
    dataType: 'STRING',
    semantics: {
      contentType: 'DIMENSION',
      semanticType: 'TEXT'
    }
  },
  {
    name: 'subscribers',
    label: {
      pl:'Wysłane',
      en: 'Subscribers'
    },
    dataType: 'NUMBER',
    semantics: {
      contentType: 'METRIC',
      semanticType: 'NUMBER',
      semanticGroup: 'NUMERIC'
    }
  },
  {
    name: 'delivered',
    label: {
      pl:'Dostarczone',
      en: 'Delivered'
    },
    dataType: 'NUMBER',
    semantics: {
      contentType: 'METRIC',
      semanticType: 'NUMBER',
      semanticGroup: 'NUMERIC'
    }
  },
  {
    name: 'hard_bounce',
    label: {
      en: 'Hard bounce',
      pl: 'Twarde odbicia'
    },
    dataType: 'NUMBER',
    semantics: {
      contentType: 'METRIC',
      semanticType: 'NUMBER',
      semanticGroup: 'NUMERIC'
    }
  },
  {
    name: 'soft_bounce',
    label: {
      en: 'Soft bounce',
      pl: 'Miękkie odbicia',
    },
    dataType: 'NUMBER',
    semantics: {
      contentType: 'METRIC',
      semanticType: 'NUMBER',
      semanticGroup: 'NUMERIC'
    }
  },
  {
    name: 'opened',
    label: {
      en: 'Opened',
      pl: 'Otwarte'
    },
    dataType: 'NUMBER',
    semantics: {
      contentType: 'METRIC',
      semanticType: 'NUMBER',
      semanticGroup: 'NUMERIC'
    }
  },
  {
    name: 'clicked',
    label: {
      en: 'Clicked',
      pl: 'Kliki'
    },
    dataType: 'NUMBER',
    semantics: {
      contentType: 'METRIC',
      semanticType: 'NUMBER',
      semanticGroup: 'NUMERIC'
    }
  },
  {
    name: 'unique_opened',
    label: {
      en: 'Unique opened',
      pl: 'Unikalne otwarcia'
    },
    dataType: 'NUMBER',
    semantics: {
      contentType: 'METRIC',
      semanticType: 'NUMBER',
      semanticGroup: 'NUMERIC'
    }
  },
  {
    name: 'unique_clicked',
    label: {
      en: 'Unique clicked',
      pl: 'Unikalne kliki'
    },
    dataType: 'NUMBER',
    semantics: {
      contentType: 'METRIC',
      semanticType: 'NUMBER',
      semanticGroup: 'NUMERIC'
    }
  },
  {
    name: 'resigned',
    label: {
      en: 'Resigned',
      pl: 'Rezygnacje'
    },
    dataType: 'NUMBER',
    semantics: {
      contentType: 'METRIC',
      semanticType: 'NUMBER',
      semanticGroup: 'NUMERIC'
    }
  }
];  
    
/**
 * Format schema structure
 *
 * @param {String} language
 * @param {Array.<Object>} fields 
 * @return {Array.<Object>}
 */
function prepareDataSchema(language, fields){ 
  if(!isEmpty(fields)){
    return fields.map(function(field){
      for(var i = DataSchema.length; i--; ){
        if(DataSchema[i].name === field.name){
          var rowLocalized = Object.create(DataSchema[i]);
          rowLocalized.label = typeof DataSchema[i].label[language] === 'undefined' ? DataSchema[i].label[DEFAULT_LANGUAGE] : DataSchema[i].label[language];    
          return rowLocalized; 
        }
      }
      return null;
    });
  }
  
  return DataSchema.map(function(row){
    var rowLocalized = Object.create(row);
    rowLocalized.label = typeof row.label[language] === 'undefined' ? row.label[DEFAULT_LANGUAGE] : row.label[language];    
    return rowLocalized; 
  });
}

/**
 * API Request
 *
 * @see https://developers.google.com/datastudio/connector/reference#getconfig
 * @param {Object} request
 * @return {Object} 
 */
function getConfig(request) {
  
  var apiKey = {
    type: 'TEXTINPUT',
    name: 'api_key',
    displayName: 'API Key'
  };
  
  var apiSecret = {
    type: 'TEXTINPUT',
    name: 'api_secret',
    displayName: 'API Secret'
  }
  
  switch(request.languageCode){
    case 'pl':
      apiKey.displayName = 'Klucz Api';
      apiSecret.displayName = 'Api Sekret';
      break;  
  }
  
  return {
    configParams: [
      apiKey,
      apiSecret
    ],
    dateRangeRequired: true
  };
};

/**
 * API Request 
 *
 * @see https://developers.google.com/datastudio/connector/reference#getschema
 * @param {Object} request
 * @return {Object}
 */
function getSchema(request) {
  return {
    schema: prepareDataSchema(Session.getActiveUserLocale())
  };
};

/**
 * API Request
 *
 * @see https://developers.google.com/datastudio/connector/reference#getdata
 * @param {Object} request
 * @return {Object}
 */
function getData(request) {    
  var fieldsSchema = prepareDataSchema(Session.getActiveUserLocale(), request.fields);
  var result = loadData( 
    request.dateRange.startDate,
    request.dateRange.endDate,
    request.configParams.api_key,
    request.configParams.api_secret,
    request.fields
  );
  
  return {
    schema: fieldsSchema,
    rows: result
  }
}

/**
 * Fetch dataset from API with local cache
 *
 * @param {String} start Date YYYY-MM-DD
 * @param {String} end Date YYYY-MM-DD
 * @param {String} apiKey
 * @param {String} apiSecret 
 * @return {Array.<Object>}
 */
function loadData( start, end, apiKey, apiSecret, fields){
  var cache = CacheService.getUserCache();
  var cacheData = cache.get('result-'+apiKey);
  var data;
  var result = [];
  
  if(null === cacheData){
    data = getCampaignList(1, apiKey, apiSecret);
    cache.put('result-'+apiKey, JSON.stringify(data), 60); 
  }else{
    data = JSON.parse(cacheData);  
  }
  
  return data.filter(function(row){
    var sent = parseDate(row.sent).getTime();
    return sent >= parseDate(start).getTime() && sent <= parseDate(end, 23, 59, 59).getTime();
  }).map(function(row){
    var values = [];
    var report = getCampaignReport(row.id_hash, apiKey, apiSecret);
    
    return {
      values: fields.map(function(field){
        if(false !== report && !isEmpty(report[field.name])){
          return report[field.name]; 
        }else{
          if(!isEmpty(row[field.name])){
            return row[field.name];
          }
        }
        
        return null;
      })
    };
  });    
}

/**
 * Parse Date with format YYYY-MM-DD or YYYY-MM-DD hh:mm:ss
 * 
 * @param {String} date
 * @param {Integer} hours
 * @param {Integer} minutes
 * @param {Integer} seconds 
 * @return {Date} 
 */
function parseDate(date, hours, minutes, seconds){
  var parsed, result = new Date();
  
  if(null !== (parsed = /^(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})$/.exec(date))){
    result.setFullYear(parsed[1], parsed[2] - 1, parsed[3]);
    result.setHours(parsed[4], parsed[5], parsed[6]);
  } else if(null !== (parsed = /^(\d{4})-(\d{2})-(\d{2})$/.exec(date))){
    result.setFullYear(parsed[1], parsed[2] - 1, parsed[3]);
    result.setHours(0, 0, 0);
  }
  
  if(!isEmpty(hours)) result.setHours(hours, 0, 0); 
  if(!isEmpty(minutes)) result.setMinutes(minutes, 0);
  if(!isEmpty(seconds)) result.setSeconds(seconds);
  
  return result;
}

/**
 * @param {mixed} value
 * @return {Boolean}
 */
function isEmpty(value){
  return typeof value === 'undefined' || null === value || '' === value; 
}
  
/**
 * API Request
 *
 * @see https://developers.google.com/datastudio/connector/reference#getauthtype
 * @return {Object}
 */
function getAuthType() {
  return {
    type: "NONE"
  };
}

/**
 * Get single campain information
 *
 * @see https://freshmail.com/developer-api/reports-from-campaign/
 * @see https://developers.google.com/apps-script/reference/cache/
 * @param {String} id
 * @param {String} ApiKey
 * @param {String} ApiSecret
 * @return {Object}
 */
function getCampaignReport(id, apiKey, apiSecret) {
  
  var cache = CacheService.getUserCache();
  var cacheData = cache.get('report-'+id+'-'+apiKey);
  var response = false;
  
  if(null === cacheData){
    try{
      response = callApi('get', apiKey, apiSecret, '/rest/reports/campaign/'+id, null);
    }catch(e){}
    cache.put('report-'+id+'-'+apiKey, JSON.stringify(response), 3600); 
  }else{
    response = JSON.parse(cacheData); 
  }
  
  return response;
}

/**
 * Get list of all campaigns
 *
 * @see https://freshmail.com/developer-api/reports-from-campaign/
 * @param {Integer} page
 * @param {String} ApiKey
 * @param {String} ApiSecret 
 */
function getCampaignList(page, apiKey, apiSecret){
  
  var result = [];
  
  try{
    var response = callApi('get', apiKey, apiSecret, '/rest/reports/campaignsList/' + page, null); 
    
    if(false !== response && response.length){
      response.forEach(function(row){
        result.push(row);
      });
    }else{
      return result;
    }
  } catch(e) {
   return result; 
  }
  
  getCampaignList( page + 1, apiKey, apiSecret ).forEach(function(row){
    result.push(row);
  });
  
  return result;
}

/**
 * Call FreshMail API
 * 
 * @see https://freshmail.com/developer-api/description-of-api/
 * @param {String} method
 * @param {String} ApiKey
 * @param {String} ApiSecret
 * @param {String} path
 * @param {mixed} data 
 * @return {Object}
 */
function callApi(method, apiKey, apiSecret, path, data){
  var jsonData = null === data ? "" : JSON.stringify(data);
  var signData = apiKey + path + jsonData + apiSecret;
  var signDigest = Utilities.computeDigest( Utilities.DigestAlgorithm.SHA_1, signData, Utilities.Charset.US_ASCII );
  var signHash = '';
  
  for (i = 0; i < signDigest.length; i++) {
    var byte = signDigest[i];
    if (byte < 0) byte += 256;
    var byteStr = byte.toString(16);
    if (byteStr.length == 1) byteStr = '0' + byteStr;
    signHash += byteStr;
  }   
  
  var requestOptions = {
    method: method,
    contentType: 'application/json',
    headers: {
      'X-Rest-ApiKey': apiKey,
      'X-Rest-ApiSign': signHash
    }
  };
  
  if(null !== data){
   requestOptions.payload = jsonData; 
  }
  
  var response = JSON.parse(UrlFetchApp.fetch('https://api.freshmail.com' + path, requestOptions));
  
  switch(response.status){
    case 'OK':
      return response.data;
    case 'ERROR':
      throw response.errors[0].code;
  }
  
  return false;
}

