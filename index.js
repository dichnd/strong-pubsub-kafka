/**
 * Created by DICH on 2/24/2017.
 */
module.exports = Adapter;

var kafka = require('kafka-node');
var ConsumerGroup = kafka.ConsumerGroup;
var Consumer = kafka.Consumer;
var Client = kafka.Client;
var Producer = kafka.Producer;
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;
var debug = require('debug')('strong-pubsub:mqtt');
var defaults = require('lodash').defaults;

function noop() {};

/**
 * The **MQTT** `Adapter`.
 *
 * **Defaults**
 *
 * - `client.options.host
 * - `client.options.port
 * - `client.options.id
 *
 * @class
 */

function Adapter(client) {
    var adapter = this;
    this.client = client;
    var options = this.options = client.options;
    this.consumerId = this.options.id || 'consumer1';

    this.transport = client.transport;
}

inherits(Adapter, EventEmitter);

Adapter.prototype.connect = function(cb) {
    var self = this;
    var adapter = this;
    var client = this.client;
    var options = this.options;
    var transport = this.transport || require('net');
    var firstConnection;
    var timer;
    var topics = this.options.topics || ['topic1', 'topic2'];

    this.kafkaClient = new Client(options.host + ':' + options.port);

    debug('connect');

    this.producer = new Producer(this.kafkaClient, { requireAcks: options.ack || 1});
    this.producer.on('ready', function () {
        self.ready = true;
        client.emit('ready')
    })
    this.producer.on('error', function (error) {
        self.ready = false;
        client.emit('error', error)
    })








    //
    // this.mqttClient = new MqttClient(function(mqttClient) {
    //     debug('mqtt client creating new connection');
    //     var connection = transport.createConnection(options.port, options.host, options);
    //     if(!firstConnection) {
    //         firstConnection = connection
    //     }
    //     return connection;
    // }, options.mqtt);
    //
    // this.mqttClient.on('message', function(topic, message, packet) {
    //     client.emit('message', topic, message, {
    //         qos: packet.qos || 0,
    //         retain: packet.retain || false
    //     });
    // });
    //
    // firstConnection.once('connect', done);
    // firstConnection.once('error', done);
    //
    // if(options.connectionTimeout) {
    //     timer = setTimeout(function() {
    //         cb(new Error('connection timeout after ' + options.connectionTimeout + 'ms'));
    //         firstConnection.close();
    //     }, options.connectionTimeout);
    // }
    //
    // function done(err) {
    //     debug('connect done');
    //     if(err) {
    //         debug('connect error %j', err);
    //     }
    //     adapter.emit('connect');
    //     clearTimeout(timer);
    //     cb(err);
    // }
}

Adapter.prototype.end = function(cb) {
    this.mqttClient.end(cb);
}

/**
 * Publish a `message` to the specified `topic`.
 *
 * @param {String} topic The topic to publish to.
 * @param messages
 * @param {Object} [options] Additional options that are not required for publishing a message.
 * @param cb
 * @param {Number} [options.qos] **default: `0`** The **MQTT** QoS (Quality of Service) setting.
 * @param {Boolean} [options.retain] **default: `false`**  The `MQTT` retain setting. Whether or not the message should be retained.
 *
 * **Supported Values**
 *
 *   - `0` - Just as reliable as TCP. Adapter will not get any missed messages (while it was disconnected).
 *   - `1` - Adapter receives missed messages at least once and sometimes more than once.
 *   - `2` - Adapter receives missed messages only once.
 *
 * @callback {Function} callback Called once the adapter has successfully finished publishing the message.
 */

Adapter.prototype.publish = function(topic, messages, options, cb) {
    var self = this;
    self.producer.send([{
        topic: topic,
        partition: options.partition || 0,
        messages: messages
    }], function (error, result) {
        cb(error, result);
    });
}

/**
 * Subscribe to the specified `topic` or **topic pattern**.
 *
 * @param topics
 * @param {Object} options The MQTT specific options.
 * @param cb
 * @param {Object} options.qos See `publish()` for `options.qos`.
 *
 * @callback {Function} callback Called once the adapter has finished subscribing.
 */

Adapter.prototype.subscribe = function(topics, options, cb) {
    var self = this;
    cb = cb || noop;
    options.qos = options.qos || 0;

    if(typeof topics === 'object') {
        Object.keys(topics).forEach(function(name) {
            topics[name] = topics[name].qos || 0;
        });
    }

    var consumerGroup = new ConsumerGroup(Object.assign({id: self.consumerId}, options), topics);
    consumerGroup.on('error', function (error) {
        self.client.emit('error', error);
    })
    consumerGroup.on('message', function (message) {
        self.client.emit('message', message.topic, message);
    })
}

/**
 * Unsubscribe from the specified `topic` or **topic pattern**.
 *
 * @param {String} topic The topic or **topic pattern** to unsubscribe.
 * @callback {Function} callback Called once the adapter has finished unsubscribing.
 * @param {Error} err An error object is included if an error was supplied by the adapter.
 */

Adapter.prototype.unsubscribe = function(topic, cb) {
    cb = cb || noop;
    this.mqttClient.unsubscribe(topic, cb);
}
