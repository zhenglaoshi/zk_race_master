const {expect} = requre('chai');
const {ZkRaceMaster} = require('../../index');

const ZK_HOST = process.env.ZOOKEEPER_PEERS;

describe('test', function(){
    it('race master', function(done){
        const task = function(is_master) {
            expect(is_master).to.be.equal(true);
            done();
        };
        new ZkRaceMaster({
            zk_server_host: ZK_SERVER_HOST, 
            session_timeout: 1000, 
            retries: 60, 
            server_path:'/',
            server_name:'server_test', 
            master_task: task, 
            node_data: null, 
            delay_time: 500
        })
    })
});