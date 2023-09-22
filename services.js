var AWS = require("aws-sdk");
require("aws-sdk/lib/maintenance_mode_message").suppress = true;
const config = require("./config");
AWS.config.update(config.aws_remote_config);
var docClient = new AWS.DynamoDB.DocumentClient();
const fs = require('fs');

const getDispositionValues = async () => {
  const params = {
    TableName: config.ADMIN_DISPOSITION_MAPPING_TABLE,
    FilterExpression: "#region = :region AND #lineOfBusiness = :lineOfBusiness",
    ExpressionAttributeNames: {
      "#region": "region",
      "#lineOfBusiness": "lineOfBusiness",
    },
    ExpressionAttributeValues: {
      ":region": config.REGION,
      ":lineOfBusiness": config.LINEOFBUSINESS,
    },
  };

  const result = await docClient.scan(params).promise();
  var CategoriesData = {};
  var SubCategoriesData = {};
  var DispData = {};
  var DispositionData = [];

  result.Items.filter((ele) => {
    DispositionData = ele.dispositionData;
  });

  //DISPOSITION_CATEGORY
  DispositionData.filter((ele) => {
    CategoriesData[ele.title] = ele.subCategories;
  });

  //DISPOSITION TITLE WITH SUBCATEGORY TITLE ARRAY
  for (const key in DispositionData) {
    var subCatTitle = [];
    DispositionData[key].subCategories.map((ele) => {
      if (ele.title != undefined) {
        subCatTitle.push(ele.title);
      }
    });
    DispData[DispositionData[key].title] = subCatTitle;
  }

  //DISPOSITION_SUBCATEGORY TITLE WITH ITEMS ARRAY
  for (const key in CategoriesData) {
    CategoriesData[key].map((ele) => {
      var itemTitle = [];
      ele.dispositionItems.filter((val) => {
        if (val.title != undefined) {
          itemTitle.push(val.title);
        }
      });
      SubCategoriesData[ele.title] = itemTitle;
    });
  }

  return {
    CategoriesData,
    SubCategoriesData,
    DispData,
  };
};

const getSubCategory = (result, item) => {
  for (const key in result.SubCategoriesData) {
    if (Object.values(result.SubCategoriesData[key]).includes(item)) {
      return key;
    }
  }
};

const getCategory = (result, item) => {
  for (const key in result.DispData) {
    if (Object.values(result.DispData[key]).includes(item)) {
      return key;
    }
  }
};

const writeFile = (dataToBeWritten) => {
  const jsonData = JSON.stringify(dataToBeWritten, null, 2);
  fs.writeFile(config.FILE_PATH, jsonData, 'utf8', (err) => {
    if(err){
      console.log("error writing JSON file: ", err);
    } else{
      console.log("Unprocessed data has been written to ", config.FILE_PATH);
    }
  })

}


module.exports = {
  getDispositionValues,
  getSubCategory,
  getCategory,
  writeFile
};
