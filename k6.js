import ws from 'k6/ws';
import { check } from 'k6';

stages: [
  { duration: '35s', target: 10000 },
  { duration: '15s', target: 10000 },
  { duration: '10s', target: 0 },
]
const TARGET_HOST = __ENV.TARGET_HOST || '127.0.0.1';

export default function () {
    const url = `ws://${TARGET_HOST}:8080/ws`;
    const res = ws.connect(url, function (socket) {
        socket.on('open', () => {
            setInterval(() => {
                socket.send('hello, extreme low latency!');
            }, 1000);
        });

        socket.on('message', (msg) => {
            check(msg, { 'is correct': (r) => r === 'hello, extreme low latency!' });
        });
    });
}