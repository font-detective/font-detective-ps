var AWS = require('aws-sdk');
var wget = require('wget-improved');
var fs = require('fs');
var cv = require('opencv');
var async = require('async');
var rimraf = require('rimraf');

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
 * AWS S3
 */

var s3 = new AWS.S3();
var defaultBucket = "font-detective-image-bucket";
var defaultFolder = "img";

// Puts a file in specified (bucket, key)
function putFileS3(filename, folder, key, bucket, metadata, callback) {
  var body = fs.createReadStream(filename);
  putS3(body, folder, key, bucket, metadata, callback);
}

// Puts data in specified (bucket, key)
// For now, this is public readable.
function putS3(body, folder, key, bucket, metadata, callback) {
  metadata.uploaded = Date.now().toString();
  var fqkey = (folder != "") ? folder + "/" + key : key;
  var s3obj = new AWS.S3({params: {Bucket: bucket, Key: fqkey, Metadata: metadata, ACL:'public-read'}});
  s3obj.upload({Body: body}).
    on("httpUploadProgress", function(evt){
        console.log((evt.loaded / evt.total).toFixed(2) + "%");
    }).
    send(callback);
}

// Gets a file in specified (bucket, key)
function getFileS3(filename, folder, key, bucket, callback) {
  var params = {Bucket: bucket, Key: fqkey};
  var file = require('fs').createWriteStream(filename);
  s3.getObject(params).createReadStream().on("finish", callback).pipe(file);
}

// Gets data from specified (bucket, key)
// returns a callback with err, data
function getS3(callback, folder, key, bucket, callback) {
  var fqkey = (folder != "") ? folder + "/" + key : key;
  var params = {Bucket: bucket, Key: fqkey};
  s3.getObject(params, callback).send();
}

// Gets the link at which the resource may be accessed
function getLinkS3(folder, key, bucket) {
  var fqkey = (folder != "") ? folder + "/" + key : key;
  return "https://s3-" + AWS.config.region + ".amazonaws.com/" + bucket.toString() + "/" + fqkey.toString();
}


/**
 * Main application code
 */

function getLocalSampleImageDir(job) {
  return __dirname + "/job/" + job.uid;
}

function getLocalSampleImagePath(job) {
  return getLocalSampleImageDir(job) + "/sample";
}

function processMessage(message) {
  // Extract job as JSON
  var job = JSON.parse(message.Body);
  console.log(job);

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
  // TODO
  console.log('TODO - cropSampleImage');
  next();
};

function classifySampleImage(job, results, next) {
  // Run the cascade files on the image
  cv.readImage(getLocalSampleImagePath(job), function(err, im) {
    fs.readdir("./classifiers/", function(err, files) {
      async.each(files, function(cascadeFile, next) {
          if (cascadeFile.match(/.*\.xml$/)) {
          console.log(cascadeFile);
          im.detectObject(cascadeFile, {neighbors: 2, scale: 2}, function(err, objects) {
            // Store the results
            results[cascadeFile] = objects;
            console.log(objects);
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
  // TODO
  console.log('TODO - storeResults');
  console.log(results);
  next();
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