var AWS = require('aws-sdk');

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
var defaultBucket = "fontdetective";
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
 
function readMessage () {

	// Grab a message from the queue
	receiveSQS(defaultQueueUrl, function(err, message) {
		// Error?
		if (err) {
			console.error(err.message);
      return readMessage();
		}

		// No messages?
		if (message == null) {
      console.log("no messages");
      return readMessage();
		}

		// Process the message
		// TODO
		console.log(message.Body);

		// Do something with the results
		// TODO

		// Delete completed message
		removeSQS(message, defaultQueueUrl, function(err, data) {
      if (err) {
        return console.error(err.message);
      }

	    console.log("Deleted message");

      // ... and loop!
      readMessage();
	  });
	});

};

readMessage();