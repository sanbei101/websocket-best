import ws from 'k6/ws';
import { check } from 'k6';

export const options = {
    vus: 10000,
    duration: '30s',
};
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