var AWS = require("aws-sdk");
const config = require("./config");
AWS.config.update(config.aws_remote_config);
var docClient = new AWS.DynamoDB.DocumentClient();
var services = require('./services');
var dataDump = require('./data');

const getAgentInteractionData = async () => {
  try{
    var allRecords = [];
    let lastEvaluatedKey = undefined;
    const batchSize = 500;
    let data;
    do {
      const params = {
        TableName: config.AGENT_INTERACTION_TABLE,
        Limit: batchSize,
        ExclusiveStartKey: lastEvaluatedKey,
        FilterExpression: "(#eventDate BETWEEN :start AND :end)",
        ProjectionExpression:"contactId, username, #eventDate, disposition, queuename, #type",
        ExpressionAttributeNames: {"#eventDate" : "timestamp", "#type": "type"},
        //change the start and end date accordingly
        ExpressionAttributeValues: {
          ":start": config.START_DATE,
          ":end": config.END_DATE,
        }
      };

      data = await docClient.scan(params).promise();
      let result = await services.getDispositionValues();
      data.Items.map((val) => {  
        // console.log(data.Items);
        let subCat = services.getSubCategory(result, val.disposition);
        let cat = services.getCategory(result, subCat); 

        let jsonContent = {
          id: val.contactId,
          agent: val.username,
          date: new Date(val.timestamp).toISOString().split("T")[0].replace(/-/g, ""),
          dispositionName: val.disposition ? val.disposition : "Disposition not selected by agent",
          dispositionSubCategory: subCat ? subCat : "Subcategory not avaiable" ,
          dispositionCategory: cat ? cat : "Category not avaiable",
          expires: Math.floor(new Date(val.timestamp) / 1000) + config.TTL_DAYS * 24 * 60 * 60,
          queue: val.queuename,
          timestamp: Math.floor(new Date(val.timestamp) / 1000),
          type: val.type
        }

        if (jsonContent) {
          allRecords.push(jsonContent);
        }
      });

      lastEvaluatedKey = data.LastEvaluatedKey;
      console.log("length of retrieved jsoncontent per batch", allRecords);
    } while (lastEvaluatedKey);
    console.log("length of retrieved jsoncontent", allRecords.length);
    return allRecords;
  } catch(err){
    console.error(err);
  }
};

const putRecords = async () => {
  // let data = await getAgentInteractionData();
  try{
    let data = dataDump.data.Items;

    const batchSize = 25;
    let batches = [];
    const errorLogs = [];

    for(let i=0; i< data.length; i+=batchSize){
      const batch = data.slice(i, i+batchSize);
      batches.push(batch);
    }

    for(const batch of batches){   
      let params = {
        RequestItems: {
          [config.DISPOSOTION_REPORTING_TABLE]: batch.map((item) => ({
            PutRequest:{
              Item: item,
            }
          }))
        }
      }

      docClient.batchWrite(params,(err, data) => {
        if(err){
          console.log(err.code);
          if(err.code === 'ValidationException'){
            console.log('simulating unprocessed items....');
            params
            .RequestItems[config.DISPOSOTION_REPORTING_TABLE]
            .map(val => {
              errorLogs.push(val.PutRequest.Item);
              services.writeFile(errorLogs);
            })
            console.log("Total unprocessed items ==> ",errorLogs.length, "pushed to json file");
            return errorLogs;
          }
        } else{
          return data
        }
      });
      
      console.log("Inserted ", batch.length, " records");
    }

    

  } catch(err){
    console.error("error", err, {"errCode":err.code,"statusCode": err.statusCode});
  }
}

putRecords().then(() => {
  console.log("successfully inserted all records");
}).catch((err) => {
  console.error("error",{"errCode":err.code,"statusCode": err.statusCode});
})






