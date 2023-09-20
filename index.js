var AWS = require("aws-sdk");
const config = require("./config");
AWS.config.update(config.aws_remote_config);
var docClient = new AWS.DynamoDB.DocumentClient();
var services = require('./services');

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
        FilterExpression: "#eventDate BETWEEN :start AND :end",
        ExpressionAttributeNames: {"#eventDate" : "timestamp"},
        ExpressionAttributeValues: {
          ":start": "2023-08-01",
          ":end": "2023-08-07"
        }
      };

      data = await docClient.scan(params).promise();
      let result = await services.getDispositionValues();

      data.Items.map((val) => {  
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
      console.log("length of retrived jsoncontent per batch", allRecords.length);
    } while (lastEvaluatedKey);
    console.log("length of retrived jsoncontent", allRecords.length);
    return allRecords;
  } catch(err){
    console.error(err);
  }
};

const putRecords = async () => {
  let data = await getAgentInteractionData();
  
  const batchSize = 25;
  let batches = [];

  for(let i=0; i< data.length; i+=batchSize){
    const batch = data.slice(i, i+batchSize);
    batches.push(batch);
  }

  for(const batch of batches){   
    let params = {
      RequestItems: {
        "TableName": batch.map((item) => ({
          PutRequest:{
            Item: item,
          }
        }))
      }
    }

    try{
      await docClient.batchWrite(params).promise();
      console.log("Inserted ", batch.length, " records");
    } catch(err){
      console.error("error", {"errCode":err.code,"statusCode": err.statusCode});
    }
  }
}

putRecords().then(() => {
  console.log("successfully inserted all records");
}).catch((err) => {
  console.error("error",{"errCode":err.code,"statusCode": err.statusCode});
})






