/**
 * Created by DICH on 2/24/2017.
 */
module.exports = Adapter;

var kafka = require('kafka-node');
var Client = kafka.Client;
var ConsumerGroup = kafka.ConsumerGroup;
var Producer = kafka.HighLevelProducer;
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;
var debug = require('debug')('strong-pubsub:mqtt');
var defaults = require('lodash').defaults;
var uid = require('uid-safe').sync;

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
    this.client = client;
    this.options = client.options;
    this.consumerId = this.options.id || 'consumer1';

    this.transport = client.transport;

    var MixinMethods = function () {}

    var self = this;
    MixinMethods.send = function (payloads, callback) {
        self.send(payloads, callback);
    }

    MixinMethods.createTopics = function (topics, async, callback) {
        self.createTopics(topics, async, callback);
    }

    this.MixinMethods = MixinMethods;
}

inherits(Adapter, EventEmitter);

Adapter.prototype.connect = function(cb) {
    var self = this;
    var client = this.client;
    var options = this.options;

    this.kafkaClient = new Client(options.host + ':' + options.port);

    debug('connect');

    this.producer = new Producer(this.kafkaClient, {
        requireAcks: options.requireAcks || 1,
        ackTimeoutMs: options.ackTimeoutMs || 100,
        partitionerType: options.partitionerType || 3
    });
    this.producer.on('ready', function () {
        self.ready = true;

        if(options.topics) {
            self.producer.createTopics(options.topics, false, function (error) {
                self.emit('ready');
                cb(null);
            })
        } else {
            self.emit('ready');
            cb(null);
        }
    })
    this.producer.on('error', function (error) {
        self.ready = false;
        self.emit('error', error)
        cb(error);
    })
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
        messages: messages,
        key: options.key
    }], function (error, result) {
        cb(error, result);
    });
}

/**
 * send multi message to multi topics
 * @param payloads
 * @param callback
 */
Adapter.prototype.send = function (payloads, callback) {
    this.producer.send(payloads, callback);
}

/**
 * request to create new topic on server
 * @param topics
 * @param async
 * @param callback
 */
Adapter.prototype.createTopics = function (topics, async, callback) {
    this.producer.createTopics(topics, async, callback);
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

    options.host = options.host || (self.options.host + ':' + self.options.port);
    options.groupId = options.groupId || uid(5);
    options.protocol = options.protocol || ['roundrobin'];
    options.fromOffset = options.fromOffset || 'latest';

    self.consumerGroup = new ConsumerGroup(Object.assign({id: self.consumerId || uid(5)}, options), topics);
    self.consumerGroup.on('error', function (error) {
        self.client.emit('error', error);
    })
    self.consumerGroup.on('message', function (message) {
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
    this.consumerGroup.leaveGroup(cb);
}
