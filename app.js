var AWS = require('aws-sdk');
var wget = require('wget-improved');
var fs = require('fs');
var cv = require('opencv');
var async = require('async');
var rimraf = require('rimraf');
var easyimg = require('easyimage');

// Set location to Ireland
AWS.config.region = "eu-west-1";

/**
 * AWS SQS
 */

var sqs =  new AWS.SQS();
var defaultQueueUrl = "https://sqs.eu-west-1.amazonaws.com/776851050546/fontdetective";

// Puts a message into the queue
function putSQS(queueUrl, value, callback) {
  var attributes = {
    uploaded: {
      DataType: "String",
      StringValue: Date.now().toString()
      }
  };
  putWithAttributesSQS(queueUrl, value, attributes, callback);
}

function putWithAttributesSQS(queueUrl, value, attributes, callback) {
  var params = {
    MessageBody: value,
    QueueUrl: queueUrl,
    MessageAttributes: attributes
  };
  sqs.sendMessage(params, callback);
}

function removeSQS(message, queueUrl, callback) {
    sqs.deleteMessage({
      QueueUrl: queueUrl,
      ReceiptHandle: message.ReceiptHandle
    }, callback);
};

function receiveSQS(queueUrl, callback) {
  sqs.receiveMessage({
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 1,
    VisibilityTimeout: 60,
    WaitTimeSeconds: 3 
  }, function(err, data) {
    if (data.Messages &&
      (typeof data.Messages[0] !== 'undefined' && typeof data.Messages[0].Body !== 'undefined')) {
      // Only one message to get...
      var message = data.Messages[0];

      // Do something useful ...
      if (callback) {
        callback(err, message);
      }
    } else {
      // Queue is empty
      callback(err, null);
    }
  });
};

/**
 * DynamoDB
 */

var dynamodb = new AWS.DynamoDB();

function putResultsDynamoDb(job, results, callback) {
  var params = {
    Item: {
      uid: {
        S: job.uid
      },
      url: {
        S: job.url
      },
      selection: {
        S: JSON.stringify(job.selection)
      },
      image: {
        S: JSON.stringify(job.image)
      },
      results: {
        S: JSON.stringify(results)
      }
    },
    TableName: 'results'
  };
  dynamodb.putItem(params, function(err, data) {
    if (err) {
      return console.error(err.message);
    }
    console.log('Stored results in database');
    callback();
  });
}


/**
 * Main application code
 */

function getLocalSampleImageDir(job) {
  return "./job/" + job.uid;
}

function getLocalSampleImagePath(job) {
  return getLocalSampleImageDir(job) + "/sample";
}

function getLocalCroppedSampleImagePath(job) {
  return getLocalSampleImagePath(job) + "-cropped";
}

function getClassifiersPath() {
  return  "./classifiers";
}

function processMessage(message) {
  // Extract job as JSON
  var job = JSON.parse(message.Body);

  var results = {};
  async.series([
    function (next) { downloadSampleImage(job, next); },
    function (next) { cropSampleImage(job, next); },
    function (next) { classifySampleImage(job, results, next); },
    function (next) { storeResults(job, results, next); },
    function (next) { deleteSampleImage(job, message, next); },
    function (done) { done(); readMessage(); }
  ], function (err) {
    if (err) {
      console.log('error!');
      return console.error(err.message);
    }
  });
};

function downloadSampleImage(job, next) {
  // Make the directory, if required
  var dir = getLocalSampleImageDir(job);
  try {
    stats = fs.lstatSync(dir);
  }
  catch (e) {
    fs.mkdir(dir);
  }

  // Download the sample image
  var download = wget.download(job.url, getLocalSampleImagePath(job), {});

  download.on('error', function(err) {
    console.error(err.message);
  });

  download.on('end', function(output) {
    next();
  });
};

function cropSampleImage(job, next) {
  easyimg.rescrop({
    src: getLocalSampleImagePath(job),
    dst: getLocalCroppedSampleImagePath(job),
    width: job.image.w,
    height: job.image.h,
    cropwidth: job.selection.w,
    cropheight: job.selection.h,
    x: job.selection.x,
    y: job.selection.y
  }).then(function(image) {
    next();
  });
};

function classifySampleImage(job, results, next) {
  // Run the cascade files on the image
  cv.readImage(getLocalCroppedSampleImagePath(job), function(err, im) {
    fs.readdir(getClassifiersPath(), function(err, files) {
      async.each(files, function(cascadeFile, next) {
          if (cascadeFile.match(/.*\.xml$/)) {
          im.detectObject(getClassifiersPath() + "/" + cascadeFile, {neighbors: 2, scale: 2}, function(err, objects) {
            if (err) {
              console.error(err.message);
            }
            // Store the results
            results[cascadeFile] = {
              objects: objects || []
            };
            next();
          });
        } else {
          next();
        }
      }, function (err) {
        if (err) {
          return console.error(err.message);
        }
        console.log('Done classifying');
        next();
      });
    });
  });
};

function storeResults(job, results, next) {
  // This needs some work,
  // for now, assume any detection is correct
  console.log(results);
  Object.keys(results).forEach(function(classifier) {
    results[classifier].found = (results[classifier].objects.length !== 0);
  });

  // Store the results in the database
  console.log(results);
  putResultsDynamoDb(job, results, next);
};

function deleteSampleImage(job, message, next) {
  // Delete local source image
  rimraf(getLocalSampleImageDir(job), function(err) {
    if (err) {
      return console.error(err.message);
    }
    console.log("Deleted temporary local files."); 
  });

  // Delete completed message
  removeSQS(message, defaultQueueUrl, function(err, data) {
    if (err) {
      return console.error(err.message);
    }
    console.log("Deleted message.");
    next();
  });
};
 
function readMessage() {
	receiveSQS(defaultQueueUrl, function(err, message) {
		// Error?
		if (err) {
			console.error(err.message);
      return readMessage();
		}

		// No messages?
		if (message == null) {
      console.log("No messages in queue.");
      return readMessage();
		}

    // Process the message
		processMessage(message);
	});
};

readMessage();