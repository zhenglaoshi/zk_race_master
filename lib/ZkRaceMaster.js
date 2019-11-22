const {EventEmitter} = require('events');
const zookeeper = require('node-zookeeper-client');
const slogger = require('node-slogger');
const ip = require('ip');
const host = ip.address();

//zkServerHost, 
// sessionTimeout, 
// retries, 
// serverName, 
// serverPath, 
// masterTask, 
// nodeData, 
// ReConnectTime, 
// isReconnected, 
// reRaceMasterdelayTime 主从重新竞争时，从节点延时竞争时间，单位ms
// unConnectedAlarmTime

class ZkRaceMaster extends EventEmitter {
    constructor({zkServerHost, sessionTimeout=3000, retries=1, spinDelay=1000 ,serverName, serverPath='/', masterTask, nodeData, reRaceMasterdelayTime = 500, ReConnectTime = 10000, isReconnected = true, unConnectedAlarmTime=5000}) {
        super();
        if(!serverName) { 
            throw new Error('缺少必要参数serverName');
        }
        if(masterTask && typeof masterTask != 'function') {
            throw new Error('master_task类型错误，必须为函数');
        }
        if(nodeData && typeof nodeData !== 'string') {
            throw new Error('node_data类型错误，必须为字符串类型');
        }
        if(!zkServerHost) {
            throw new Error('缺少必要参数zk_server_host');
        }
        if(isReconnected && ReConnectTime<sessionTimeout) {
            throw new Error('重连时间必须大约会话超时时间');
        }
        this._zkClient = zookeeper.createClient(zkServerHost, {sessionTimeout: sessionTimeout, retries, spinDelay});
        this._zk_host = zkServerHost;
        this._session_timeout = sessionTimeout;
        this._retries = retries;
        this._spinDelay = spinDelay;
        this._server_name = serverName;
        this._path = serverPath;
        this._zk_node = this._path + this._server_name;
        this._master_task = masterTask || function(){};
        this._rerace_delay_time = reRaceMasterdelayTime;
        this._node_data = nodeData || (host+':'+process.pid);
        this._is_master = false;
        this._is_connected = false;
        this._reconnect_time = ReConnectTime;
        this._is_reconnected = isReconnected;
        this._unconnected_alarm_time = unConnectedAlarmTime;
        this._reconnectTimer = null;
        this._init();
    }
	
    _init() {
        const _this = this;
        _this._zkClient.on('state', function (state) {
            if (state === zookeeper.State.DISCONNECTED ) {
                _this._is_master = false;
                slogger.debug('zookeeper断开连接');
                _this._is_connected = false;
                _this._reconntect();
                _this.emit(ZkRaceMaster.EVENT_ZK_DISCONNECTED);
            } else if(state === zookeeper.State.EXPIRED) {
                _this._is_master = false;
                slogger.error('zookeeper连接超时');
                _this._is_connected = false;
                _this.emit(ZkRaceMaster.EVENT_ZK_EXPIRED);
            } else if(state === zookeeper.State.SYNC_CONNECTED){
                slogger.debug('连接zookeeper成功');
                _this.emit(ZkRaceMaster.EVENT_ZK_CONNECTED);
                _this._is_connected = true;
                _this._raceMaster();
            } else {
                //slogger.debug(state);
            }
        });
        setTimeout(function(){
            if(!_this._is_connected) {
                slogger.warn('在期望的时间内没有和zookeeper服务connect成功');
                _this.emit(ZkRaceMaster.EVENT_UNCONNECTED_IN_EXPECTED_TIME);
            }
        }, _this._unconnected_alarm_time);
        _this._zkClient.connect();	
    }
    _reconntect() {
        const _this = this;
        if(!_this._is_reconnected) {
            return;
        }
        if(!_this._is_connected) {
            _this._zkClient.close();
            _this._zkClient = null;
            _this._zkClient = zookeeper.createClient(_this._zk_host, {sessionTimeout: _this._session_timeout, retries: _this._retries, spinDelay: _this._spinDelay});
            _this._init();
        }
    }
    _raceMaster() {
        const _this = this;
        _this._is_master = false;
        _this._lisentenNode();
        _this._zkClient.create(_this._zk_node, Buffer.from(_this._node_data), zookeeper.CreateMode.EPHEMERAL, function(err) {
            if(err) {
                slogger.debug('竞争master失败', err);
                _this._is_master = false;
                _this.emit(ZkRaceMaster.EVENT_CREATE_NODE_ERROR, err);
            } else {
                _this._is_master = true;
            }
            _this._master_task(_this._is_master);
        });
    }
	
    _lisentenNode() {
        const _this = this;
        _this._zkClient.getData(_this._zk_node, function(event){
            slogger.debug(_this._zk_node, ' 节点变动：', event.name);
            if(event.name === 'NODE_DELETED') {
                if(_this._is_master)
                {
                    _this._raceMaster();
                } else {
                    setTimeout(function(){
                        _this._raceMaster();
                    }, _this._rerace_delay_time);
                }
                _this.emit(ZkRaceMaster.EVENT_NODE_DELETE);
            }else if(event.name === 'NODE_CREATED') {
                _this.emit(ZkRaceMaster.EVENT_NODE_CREATE);
            } else if(event.name === 'NODE_DATA_CHANGED') {
                _this.emit(ZkRaceMaster.EVENT_NODE_UPDATE);
            } else {
                _this.emit(event.name);
            }
        }, function(err, data, stat) {
            if(err) {
                slogger.warn('获取节点 '+_this._zk_node+' 数据错误：', err);
                _this.emit(ZkRaceMaster.EVENT_GET_NODE_DATA_ERROR, err);
            }
        });
    }
	
    // existsMaster(callback) {
    //     const _this = this;
    //     _this._zkClient.exists(_this._zk_node, function(err, stat) {
    //         if(err) {
    //             slogger.error('获取master信息失败',err);
    //             return callback(err);
    //         }
    //         callback(err, stat);
    //     });
    // }
	
    // getMasterInfo(callback) {
    //     const _this = this;
    //     _this._zkClient.getData(_this._zk_node, null, function(err, data, stat){
    //         if(err) {
    //             slogger.error('获取数据节点失败', err);
    //             return callback(err);
    //         }
    //         callback(false, data, stat);
    //     });
    // }
	
    // _getMasterSessionId(callback) {
    //     this.getMasterInfo(function(err, data, stat) {
    //         if(err) {
    //             slogger.error('获取节点信息失败',err);
    //             return callback(err);
    //         }
    //         const session_id = stat.ephemeralOwner.toString('hex');
    //         callback(false, session_id);
    //     });
        
    // }
	
    isMaster() {                                                                                                                             
        return this._is_master;
    }

    isConnected(){
        return this._is_connected;
    }

    // _checkIsMaster(callback) {
    //     const _this = this;
    //     _this._getMasterSessionId(function(err, session_id) {
    //         if(err) {
    //             slogger.error('获取节点信息失败',err);
    //             return callback(err);
    //         }
			
    //         const current_session_id = _this._zkClient.getSessionId().toString('hex');
    //         return callback(false, current_session_id === session_id);
    //     });
    // }
}

ZkRaceMaster.EVENT_CREATE_NODE_ERROR = 'EVENT_CREATE_NODE_ERROR';
ZkRaceMaster.EVENT_NODE_DELETE = 'EVENT_NODE_DATA_DELETE';
ZkRaceMaster.EVENT_NODE_CREATE = 'EVENT_NODE_DATA_CREATE';
ZkRaceMaster.EVENT_NODE_UPDATE = 'EVENT_NODE_DATA_CHANGE';
ZkRaceMaster.EVENT_GET_NODE_DATA_ERROR = 'EVENT_GET_NODE_DATA_ERROR';
ZkRaceMaster.EVENT_ZK_CONNECTED = 'EVENT_ZOOKEEPER_CONNECTED';
ZkRaceMaster.EVENT_ZK_DISCONNECTED = 'EVENT_ZOOKEEPER_DISCONNECTED';
ZkRaceMaster.EVENT_ZK_EXPIRED = 'EVENT_ZOOKEEPER_EXPIRED';
ZkRaceMaster.EVENT_UNCONNECTED_IN_EXPECTED_TIME = 'EVENT_UNCONNECTED_IN_EXPECTED_TIME';



module.exports = ZkRaceMaster;