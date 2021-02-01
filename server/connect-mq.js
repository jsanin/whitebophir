// connect-mq.js
const amqp = require('amqp-connection-manager')
    , log = require("./log.js").log
    , config = require("./configuration");

// Read HOSTNAME. When running multiple instances, for example in k8s, HOSTNAME can get the name of the current pod.
// When multiple instances occur, it is better to take the name of pod when writing a log or establishing a connection.
// If there is a problem, it is also better to locate which pod appears.
const hostName = process.env.HOSTNAME

// Queue property settings
// Generally speaking, it's better to set up a queue to delete autoDelete automatically.
// When the link is disconnected, the queue will also be deleted, which will not produce a lot of useless queues.
// durable is used for persistence, and it's best to set it to non-persistence

const queueAttr = {autoDelete: false, durable: true}

// Define a reference to channel. When links are established,
// all methods can obtain channel methods by referencing channel.
let channel = null;
let connection = null;


// Functions that send messages to queues
function publishMessage (msg) {

    var msgToSend = JSON.stringify(msg)

    channel.sendToQueue(config.QUEUE_NAME, msgToSend)
        .then(function() {
            return log("Message was sent!", { 'board': msg.name });
        })
        .catch(function(err) {
            log("Message was rejected:", err.stack);
            channel.close();
            connection.close();
        });
}


// Main functions linking rabbitMQ
function connectRabbitMq () {

    // Create a connetion manager
    var conn = amqp.connect([config.RABBIT_MQ_URI], {
        // Setting the connection_name property allows you to see which instance the link came from on the
        // UI of the rabbitMQ console
        clientProperties: {
            connection_name: hostName
        }
    });
    conn.on('connect', function() {
        log('Connected to rabbitmq!');
    });
    conn.on('disconnect', function(err) {
        log('Disconnected from rabbitmq', err.stack);
    });

    // Create a channel wrapper
    var channelWrapper = conn.createChannel({
        json: true,
        setup: function(channel) {
            // `channel` here is a regular amqplib `ConfirmChannel`.
            return channel.assertQueue(config.QUEUE_NAME, queueAttr);
        }
    });

    channel = channelWrapper;
    connection = conn;

}


module.exports = {
    connectRabbitMq,
    publishMessage
}