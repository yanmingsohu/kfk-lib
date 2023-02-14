/* 该文件单独运行 */

var kafka = require('kafka-node'),
    HighLevelConsumer = kafka.HighLevelConsumer,
    HighLevelProducer = kafka.HighLevelProducer;

// conect zookeeper
var client = new kafka.Client('localhost:2181'); // 192.168.7.228:2181

var topic = 'node_test';
var count = 2;
var rets = 0;

createPro();
createCli();


function createCli() {
    var options = { autoCommit: true, fromBeginning: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024 };
    // 服务端会创建 group 并记录每个 group 的 offset，下次连接时不会重复发送 group 已经读取的数据
    options.groupId = 'zy_kafka_node_log_srv';

    var consumer = new HighLevelConsumer( client, [ { topic: topic } ], options );

    consumer.on('message', function (message) {
        console.info('recv1', message.offset, message.value);
    });

    /* consumer.on('message', function (message) {
        console.info('recv2', message);
    }); ok! */

    // c error: { [FailedToRebalanceConsumerError: Exception: NODE_EXISTS[-110]] message: 'Exception: NODE_EXISTS[-110]' }
    // 服务器执行这些操作时操作不是安全的，一次rebalance操作可能会把分区分配给刚刚从分区拥有者注册表中删除的消费者，
    // 这样客户端处就会报这个错误，但经过多次尝试后总能够达到一个稳定的状态，所以这个异常是可以忽略的。
    consumer.on('error', function (err) {
    	console.error('cons error:', err);
        consumer.stop();
        createCli();
    });
}


function createPro(_ready1) {
    var producer = new HighLevelProducer(client);

    producer.on('ready', function () {
        send('producer', function() {
            send('ready', _ready1);
        });

        //setInterval(function() { send("hello " + Math.random()); }, 1000);
    });

    producer.on('error', function(err) {
        console.log("pro err:", err);
    });


    setInterval(function() { send("hello " + Math.random()); }, 1000);

    function send(message, _ready2) {
        producer.send([
            {topic: topic, messages: [message] }
        ], function (err, data) {
            // data = { partition: offset }
            if (err) {
                console.error('prod err:', err);
            } else {
                console.log('send', data, message);
                _ready2 && _ready2();
            }
        });
    }
}



console.log('run');