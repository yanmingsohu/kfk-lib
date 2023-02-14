# kfk 封装库


## windows 编译时安装 vs2012

> 编译时出错：*MSBUILD : error MSB3411: 未能加载 Visual C++ 组件“VCBuild.exe”*
> 则运行如下命令：
`npm config set msvs_version 2012 --global`


# install

`npm install kfk-lib --save`


## API

引入库:

`var kfk = require('kfk-lib');`
`var client = kfk();`

发送数据:

`client.send(Key, Object);`

接收数据:

```
client.recv(Key, function(ret) {
    d.offset,       在消息队列中的偏移
    d.partition,    消息分区
    d.value,        发送的数据体
    d.key
});
```

创建一个主题:

`client.createTopic(topic_name, created_cb)`

设置偏移, 读取更早的数据:

`client.setOffset(topic, partition, _offset)`