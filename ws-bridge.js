#!/usr/bin/env node
/**
 * WebSocket → MQTT Bridge
 * 浏览器通过 WebSocket 连接本服务，本服务转发到 MQTT Broker
 */
const WebSocket = require('ws');
const mqtt = require('mqtt');

const WS_PORT = 8788;          // 浏览器连接本服务的端口
const MQTT_BROKER = 'mqtt://172.16.18.95';
const MQTT_PORT = 1883;
const MQTT_USER = 'nic9dzyn';
const MQTT_PASS = '587212ee97e29d80471f33cf333b83a5374ede65adccd16338d6bebebf4be224';

const wss = new WebSocket.Server({ port: WS_PORT });
console.log(`🌱 WS Bridge 已启动，监听 ws://0.0.0.0:${WS_PORT}`);

wss.on('connection', (ws, req) => {
  const clientId = 'bridge_' + Math.random().toString(16).slice(2, 8);
  console.log(`[${clientId}] 浏览器已连接`);

  const mqttClient = mqtt.connect(MQTT_BROKER, {
    port: MQTT_PORT,
    username: MQTT_USER,
    password: MQTT_PASS,
    clientId,
    reconnectPeriod: 5000,
    connectTimeout: 10000,
  });

  // 存储浏览器订阅的主题
  const subscriptions = new Map();

  mqttClient.on('connect', () => {
    console.log(`[${clientId}] MQTT 已连接`);
    ws.send(JSON.stringify({ type: 'connected' }));

    // 重发之前浏览器端的订阅
    for (const [topic, qos] of subscriptions) {
      mqttClient.subscribe(topic, { qos }, (err) => {
        if (err) ws.send(JSON.stringify({ type: 'error', msg: `订阅失败: ${err.message}` }));
      });
    }
  });

  mqttClient.on('message', (topic, payload) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'message',
        topic,
        payload: payload.toString(),
        ts: new Date().toISOString(),
      }));
    }
  });

  mqttClient.on('error', (err) => {
    console.log(`[${clientId}] MQTT 错误:`, err.message);
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'error', msg: err.message }));
    }
  });

  mqttClient.on('offline', () => {
    console.log(`[${clientId}] MQTT 离线`);
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'offline' }));
    }
  });

  // 解析浏览器发来的消息
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.type === 'subscribe') {
        const { topic, qos = 1 } = msg;
        subscriptions.set(topic, qos);
        mqttClient.subscribe(topic, { qos }, (err) => {
          if (err) {
            ws.send(JSON.stringify({ type: 'error', msg: `订阅失败: ${err.message}` }));
          } else {
            console.log(`[${clientId}] 订阅: ${topic}`);
            ws.send(JSON.stringify({ type: 'subscribed', topic }));
          }
        });

      } else if (msg.type === 'unsubscribe') {
        const { topic } = msg;
        subscriptions.delete(topic);
        mqttClient.unsubscribe(topic);
        ws.send(JSON.stringify({ type: 'unsubscribed', topic }));

      } else if (msg.type === 'publish') {
        const { topic, payload, qos = 1 } = msg;
        mqttClient.publish(topic, payload, { qos }, (err) => {
          if (err) {
            ws.send(JSON.stringify({ type: 'error', msg: `下发失败: ${err.message}` }));
          } else {
            console.log(`[${clientId}] 下发: ${topic} → ${payload}`);
            ws.send(JSON.stringify({ type: 'published', topic, payload }));
          }
        });

      } else if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (e) {
      ws.send(JSON.stringify({ type: 'error', msg: `消息解析错误: ${e.message}` }));
    }
  });

  ws.on('close', () => {
    console.log(`[${clientId}] 浏览器断开`);
    mqttClient.end();
  });

  ws.on('error', (err) => {
    console.log(`[${clientId}] WS 错误:`, err.message);
  });
});
