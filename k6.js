import ws from 'k6/ws';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

export const options = {
    stages: [
        { duration: '10s', target: 10000 },
        { duration: '20s', target: 20000 },
        { duration: '30s', target: 30000 },
    ]
};

const TARGET_HOST = __ENV.TARGET_HOST || '127.0.0.1';
const MSG = 'hello, extreme low latency!';

const wsMsgLatency = new Trend('ws_msg_latency', true);
const wsMsgTimeout = new Counter('ws_msg_timeout');
const wsMsgMatched = new Counter('ws_msg_matched');
const wsMsgUnexpected = new Counter('ws_msg_unexpected');

export default function () {
    const url = `ws://${TARGET_HOST}:8080/ws`;

    ws.connect(url, function (socket) {
        const pending = [];

        socket.on('open', () => {
            socket.setInterval(() => {
                pending.push(Date.now());
                socket.send(MSG);
            }, 5000);
        });

        socket.on('message', (msg) => {
            check(msg, {
                'is correct': (r) => r === MSG,
            });

            if (msg !== MSG) {
                wsMsgUnexpected.add(1);
                return;
            }

            if (pending.length === 0) {
                wsMsgUnexpected.add(1);
                return;
            }

            const start = pending.shift();
            wsMsgLatency.add(Date.now() - start);
            wsMsgMatched.add(1);
        });

        socket.setInterval(() => {
            const now = Date.now();
            while (pending.length > 0 && now - pending[0] > 15000) {
                pending.shift();
                wsMsgTimeout.add(1);
            }
        }, 1000);
    });
}