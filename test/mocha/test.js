const {expect} = require('chai');
const {ZkRaceMaster} = require('../../index');

const ZK_HOST = process.env.ZOOKEEPER_PEERS;

describe('test', function(){
    it('race master', function(done){
        const task = function(is_master) {
            expect(is_master).to.be.equal(true);
            done();
        };
        new ZkRaceMaster({
            zkServerHost: ZK_HOST, 
            sessionTimeout: 1000, 
            retries: 60, 
            serverPath:'/',
            serverName:'server_test', 
            masterTask: task, 
            nodeData: null, 
            ReRaceMasterdelayTime: 500
        });
    });
});
