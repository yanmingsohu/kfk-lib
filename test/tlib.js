var kfk = require('../lib/kfk-lib.js');

var client = kfk();
var a = -1;

// 必须在一开始调用, 如果消息不能重复处理则设置为相同的 id
//client.setGroupId('log-server');

// client.setOffset('node_test', 0, 1300);


client.recv('node_test', function(d) {
  console.log('recv', d.offset, d.partition, d.value, d.key.toString());
  next();
});


next();

function next() {
  if (++a >= 10) {
    console.log('ok exit');
    process.exit(0);
    return;
  }

  client.send('node_test', a + ' hello test ');
}
