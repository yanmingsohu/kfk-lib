var config = {
  logLevel: 'ALL',
  
  kfk_server: {
    zookeeper_server  : '192.168.7.22:2181', //'192.168.7.12:2181',
    connect_delay     : 10 * 1000,
    conn_timeout      : 30 * 1000,

    consumer_option   : { 
      autoCommit      : true, 
      fromBeginning   : false, 
      fetchMaxWaitMs  : 1000, 
      fetchMaxBytes   : 1024*1024 
    }
  }

};

module.exports = config;