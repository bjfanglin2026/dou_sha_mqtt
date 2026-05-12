#!/usr/bin/env node
const mqtt = require('mqtt');

const CONFIG = {
  broker: 'mqtt://mqtt.zacloud.bjzatx.com',
  port: 1883,
  username: 'nic9dzyn',
  password: '587212ee97e29d80471f33cf333b83a5374ede65adccd16338d6bebebf4be224',
  clientId: 'cmd_' + Math.random().toString(16).slice(2, 8),
  IMEI: '861241058615202',
};
CONFIG.reportTopic = `/push/device/selfservice/result/${CONFIG.IMEI}`;
CONFIG.sendTopic   = `/zayr/device/selfservice/nic9dzyn/${CONFIG.IMEI}`;

const action = process.argv[2];

if (!action) {
  console.log('用法: node mqtt-cmd.js <命令> [参数]');
  console.log('');
  console.log('命令:');
  console.log('  status              查看连接状态');
  console.log('  order [订单号]      下发订单确认（订单号默认随机1-65535）');
  console.log('  cancel [订单号]    取消订单确认');
  console.log('  raw <json>          发送自定义JSON');
  console.log('  sub                 仅连接并监听，5秒后退出');
  process.exit(0);
}

const client = mqtt.connect(CONFIG.broker, {
  port: CONFIG.port, username: CONFIG.password,
  password: CONFIG.password, clientId: CONFIG.clientId,
  reconnectPeriod: 5000, connectTimeout: 10000,
});

const timeout = setTimeout(() => {
  console.log('⏰ 超时，退出');
  client.end();
  process.exit(0);
}, 15000);

client.on('connect', () => {
  if (action === 'status') {
    console.log(`✅ 已连接: ${CONFIG.broker}:${CONFIG.port}`);
    console.log(`📥 订阅: ${CONFIG.reportTopic}`);
    console.log(`📤 下发: ${CONFIG.sendTopic}`);
    console.log(`🖥  设备: ${CONFIG.IMEI}`);
    client.end();
    process.exit(0);
  }

  client.subscribe(CONFIG.reportTopic, { qos: 1 }, (err) => {
    if (err) { console.log('❌ 订阅失败:', err.message); client.end(); process.exit(1); }
    console.log(`✅ 已连接，已订阅: ${CONFIG.reportTopic}`);

    if (action === 'order') {
      const no = parseInt(process.argv[3]) || Math.floor(Math.random() * 65535) + 1;
      const payload = { dingdanqueren: 1, dingdanshuliang: 1, dingdanhao: no };
      client.publish(CONFIG.sendTopic, JSON.stringify(payload), { qos: 1 }, (err) => {
        if (err) { console.log('❌ 下发失败:', err.message); } 
        else { console.log(`📤 已下发订单 #${no}:`, JSON.stringify(payload)); }
        setTimeout(() => { client.end(); process.exit(0); }, 500);
      });
    } else if (action === 'cancel') {
      const no = parseInt(process.argv[3]) || 0;
      const payload = { dingdanqueren: 0, dingdanshuliang: 0, dingdanhao: no };
      client.publish(CONFIG.sendTopic, JSON.stringify(payload), { qos: 1 }, (err) => {
        if (err) { console.log('❌ 下发失败:', err.message); }
        else { console.log(`📤 已取消订单 #${no}:`, JSON.stringify(payload)); }
        setTimeout(() => { client.end(); process.exit(0); }, 500);
      });
    } else if (action === 'raw') {
      const jsonStr = process.argv.slice(3).join(' ');
      try {
        const payload = JSON.parse(jsonStr);
        client.publish(CONFIG.sendTopic, JSON.stringify(payload), { qos: 1 }, (err) => {
          if (err) { console.log('❌ 下发失败:', err.message); }
          else { console.log(`📤 已下发: ${JSON.stringify(payload)}`); }
          setTimeout(() => { client.end(); process.exit(0); }, 500);
        });
      } catch(e) { console.log('❌ JSON 格式错误:', e.message); client.end(); process.exit(1); }
    } else if (action === 'sub') {
      console.log('监听中，5秒后自动退出...');
    } else {
      console.log('❌ 未知命令:', action);
      client.end();
      process.exit(1);
    }
  });
});

client.on('message', (topic, msg) => {
  const raw = msg.toString();
  const ts = new Date().toLocaleTimeString('zh-CN', { hour12: false });
  console.log(`📨 [${ts}] [${topic}] ${raw}`);
  try {
    const d = JSON.parse(raw);
    if (d.imei) {
      const f = ['jiqikongxian','ddsb','ddwc','queliao','doushazhunbei','dingdanshoudao','dingdanjiagong'];
      const l = {jiqikongxian:'机器空闲',ddsb:'订单失败',ddwc:'订单完成',queliao:'缺料',doushazhunbei:'设备准备',dingdanshoudao:'订单收到',dingdanjiagong:'机器加工'};
      const active = f.filter(k => d[k]==1).map(k=>l[k]+'=1');
      if(active.length) console.log('   状态: ' + active.join(', '));
    }
  } catch(e){}
});

client.on('error', (err) => { console.log('❌', err.message); process.exit(1); });
