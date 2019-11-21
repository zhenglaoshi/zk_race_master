const {EventEmitter} = require('events');
const zookeeper = require('node-zookeeper-client');
const slogger = require('node-slogger');
const ip = require('ip');
const host = ip.address();

//zk_server_host, session_timeout, retries, server_name, _master_task, _node_data, delay_time 主从重新竞争时，从节点延时竞争时间，单位ms

class ZkRaceMaster extends EventEmitter {
    constructor({zk_server_host, session_timeout=3000, retries=0, spinDelay=1000 ,server_name, server_path='/', master_task, node_data, delay_time = 500}) {
        super();
        if(!server_name) { 
            throw new Error('缺少必要参数server_name');
        }
        if(master_task && typeof master_task != 'function') {
            throw new Error('master_task类型错误，必须为函数');
        }
        // if(node_data && typeof node_data !== 'string') {
        //     throw new Error('node_data类型错误，必须为字符串类型');
        // }
        if(!zk_server_host) {
            throw new Error('缺少必要参数zk_server_host');
        }
        this._zkClient = zookeeper.createClient(zk_server_host, {sessionTimeout: session_timeout, retries, spinDelay});
        this._server_name = server_name;
        this._path = server_path;
        this._zk_node = this._path + this._server_name;
        this._master_task = master_task || function(){};
        this._delay_time = delay_time;
        this._is_master = false;
        try {
            this._node_data = Buffer.from(node_data || (host+':'+process.pid));
        } catch(err) {
            throw new Error('节点数据格式错误');
        }
        this._init();
    }
	
    _init() {
        const _this = this;
        _this._zkClient.on('state', function (state) {
            if (state === zookeeper.State.DISCONNECTED ) {
                _this._is_master = false;
                slogger.debug('zookeeper断开连接');
                _this.emit(ZkRaceMaster.EVENT_ZK_CONNECTED);
            } else if(state === zookeeper.State.EXPIRED) {
                _this._is_master = false;
                slogger.error('zookeeper连接超时');
                _this.emit(ZkRaceMaster.EVENT_ZK_EXPIRED);
            } else if(state === zookeeper.State.SYNC_CONNECTED){
                slogger.debug('连接zookeeper成功');
                _this.emit(ZkRaceMaster.EVENT_ZK_CONNECTED);
                _this._raceMaster();
            } else {
                //slogger.debug(state);
            }
        });
		
        _this._zkClient.connect();
		
    }
    _raceMaster() {
        const _this = this;
        _this._is_master = false;
        _this._lisentenNode();
        _this._zkClient.create(_this._zk_node, _this._node_data, zookeeper.CreateMode.EPHEMERAL, function(err) {
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
                    }, _this._delay_time);
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
                slogger.error('获取节点 '+_this._zk_node+' 数据错误：', err);
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
ZkRaceMaster.EVENT_ZK_CONNECTED = 'EVENT_ZOOKEEPER_CONNECTED';
ZkRaceMaster.EVENT_ZK_DISCONNECTED = 'EVENT_ZOOKEEPER_DISCONNECTED';
ZkRaceMaster.EVENT_ZK_EXPIRED = 'EVENT_ZOOKEEPER_EXPIRED';
ZkRaceMaster.EVENT_GET_NODE_DATA_ERROR = 'EVENT_GET_NODE_DATA_ERROR';

module.exports = ZkRaceMaster;