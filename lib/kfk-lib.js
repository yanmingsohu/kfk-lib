var Event  = require('events').EventEmitter;
var kafka  = require('kafka-node');
var logger = require('logger-lib');


module.exports = kfk;


function kfk(_user_option) {
  var conf_lib = require('configuration-lib');
  var option = conf_lib.load().kfk_server;

  if (_user_option) {
    option = conf_lib.extends(option, _user_option);
  }

  // 服务端会创建 group 并记录每个 group 的 offset，
  // 下次连接时不会重复发送 group 已经读取的数据
  if (!option.consumer_option.groupId)
    option.consumer_option.groupId = gen_group_id();

  var cli_opt       = { sessionTimeout: option.connect_delay - 1500 };
  var client        = null;
  var offset        = null;
  var consumer      = null;
  var wait_topcis   = [];
  var recv_event    = new Event();
  var produce_queue = send_queue();


  re_init_client();


  function re_init_client() {
    if (client) client.close();
    produce_queue.reset();
    offset   = null;
    consumer = null;

    logger.info('kfk group id:', option.consumer_option.groupId);
    client = new kafka.Client(option.zookeeper_server, null, cli_opt);
    createCliDelay();

    client.on('error', function(err) {
      logger.error('kfk client err:', err, ">>> reconnecting...");
    });

    if (option.conn_timeout > 0) {
      var tid = setTimeout(function() {
        client.emit('error', new Error("connect timeout."));
      }, option.conn_timeout);

      client.on('ready', function() {
        clearTimeout(tid);
      });
    }
  }

  function _recv(topic, message_cb) {
    recv_event.on(topic, message_cb);
    wait_topcis.push({ topic: topic });

    if (consumer == null) {
      createCliDelay();
    } else {
      consumer.addTopics([topic]);
    }
  }

  function _send(topic, message) {
    produce_queue.send(topic, message);

    if (produce_queue.needProducer()) {
      createPro(
        function(_producer) {
          produce_queue.setProducer(_producer);
        }, 
        function() {
          produce_queue.reset();
        }
      );
    }
  }

  // 补丁: 相同 group 的客户端频繁连接会导致 FailedToRebalanceConsumerError
  // 并且即使下次连接上，也会收不到消息，给服务器足够的时间释放上一个连接
  function createCliDelay() {
    setTimeout(function() { createCli(); }, option.connect_delay);
  }

  function createCli() {
    if (consumer) return;
    if (wait_topcis.length < 1) return;

    var this_client = client;
    consumer = new kafka.HighLevelConsumer(client, wait_topcis, option.consumer_option);

    consumer.on('error', function (err) {
      if (this_client === client) {
        logger.error('consumer error:', err.message, ">>> reconnection");
        this_client = 'has re init.';
        consumer.close();
        re_init_client();
      }
    });

    consumer.on('message', function(data) {
      // console.log(']', data.topic, ">>", data.value);
      recv_event.emit(data.topic, data);
    });
  }

  var producer_creating = false;

  function createPro(_ready, _error) {
    if (producer_creating) return;
    producer_creating = true;

    var _producer = new kafka.HighLevelProducer(client);

    _producer.on('ready', function () {
      producer_creating = false;
      _ready && _ready(_producer);
    });

    _producer.on('error', function (err) {
      producer_creating = false;
      logger.error("producer err:", err.message);
      _error && _error(_producer);
    });

    // 太 TM 坑了!!
    if (_producer.ready) {
      _producer.emit('ready');
    }
  }

  function _set_off(topic, partition, _offset) {
    if (!offset) offset = new kafka.Offset(client);

    offset.commit(option.consumer_option.groupId,
        [ { topic: topic, offset: _offset, partition: partition } ], 
        function(err) { if (err) { logger.error(err); } } );
  }

  function createTopic(topic_name, created_cb) {
    if (produce_queue.needProducer()) {
      if (!created_cb) {
        created_cb = function(err, msg) {
          logger.info('createTopic', err, msg);
        };
      }
      createPro(
        function(_producer) {
          _producer.createTopics(topic_name, created_cb);
        }
      );
    }
  }

  return {
    send        : _send,
    recv        : _recv,
    setOffset   : _set_off,
    createTopic : createTopic
  }
}


function gen_group_id(oa) {
  var os = require('os');
  var crypto = require('crypto');
  var shasum = crypto.createHash('md5');

  shasum.update(os.hostname());
  shasum.update(os.platform());
  shasum.update(os.release());
  shasum.update('cpu' + os.cpus());
  shasum.update(JSON.stringify(os.networkInterfaces()));

  if (oa) {
    shasum.update(JSON.stringify(oa));
  }

  return shasum.digest('hex');
}


function send_queue() {
  var producer = null;
  var queue = [];

  function send(topic, msg) {
    var sender = getSendHandle(topic, msg);

    if (producer) {
      sender();
    } else {
      queue.push(sender);
    }
  }

  function getSendHandle(topic, msg) {
    var send_data = [ {topic: topic, messages: [msg] } ];

    var send_handle = function() {
      producer.send(send_data, function (err, data) {
        // data = { partition: offset }
        if (err) {
          logger.error('producer send err:', err.message);
        }
      });
    }
    return send_handle;
  }

  // 该方法被调用时, producer 状态为 ready
  function setProducer(p) {
    if (!p) return;
    producer = p;

    queue.forEach(function(sender) {
      sender();
    });
    queue = [];
  }

  function reset() {
    producer = null;
  }

  function needProducer() {
    return null == producer;
  }

  return {
    send         : send,
    setProducer  : setProducer,
    reset        : reset,
    needProducer : needProducer
  };
}