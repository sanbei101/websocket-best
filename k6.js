import ws from 'k6/ws';
import { check } from 'k6';

export const options = {
    vus: 10000,
    duration: '30s',
};

export default function () {
    const url = 'ws://127.0.0.1:8080/ws';
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