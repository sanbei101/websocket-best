import ws from 'k6/ws';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

export const options = {
    stages: [
        { duration: '10s', target: 3000 },
        { duration: '10s', target: 4000 },
        { duration: '10s', target: 5000 },
    ],
};

const TARGET_HOST = __ENV.TARGET_HOST || '127.0.0.1';

const wsMsgLatency = new Trend('ws_msg_latency', true);
const wsMsgTimeout = new Counter('ws_msg_timeout');

export default function () {
    const url = `ws://${TARGET_HOST}:8080/ws`;

    ws.connect(url, function (socket) {
        let seq = 0;
        const pending = new Map();

        socket.on('open', () => {
            socket.setInterval(function () {
                const id = `${__VU}-${__ITER}-${seq++}`;
                const now = Date.now();

                const payload = {
                    id,
                    body: 'hello, extreme low latency!',
                    sentAt: now,
                };

                pending.set(id, now);
                socket.send(JSON.stringify(payload));
            }, 1000);
        });

        socket.on('message', (msg) => {
            let data;

            try {
                data = JSON.parse(msg);
            } catch (e) {
                return;
            }

            check(data, {
                'body is correct': (r) => r.body === 'hello, extreme low latency!',
            });

            if (data.id && pending.has(data.id)) {
                const start = pending.get(data.id);
                wsMsgLatency.add(Date.now() - start);
                pending.delete(data.id);
            }
        });

        socket.setInterval(function () {
            const now = Date.now();
            for (const [id, start] of pending) {
                if (now - start > 5000) {
                    wsMsgTimeout.add(1);
                    pending.delete(id);
                }
            }
        }, 1000);

        socket.setTimeout(function () {
            socket.close();
        }, 30000);
    });
}