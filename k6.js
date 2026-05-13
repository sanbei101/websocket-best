import ws from 'k6/ws';
import { check } from 'k6';

export const options = {
    stages: [
        { duration: '10s', target: 1000 },
        { duration: '10s', target: 2000 },
        { duration: '10s', target: 3000 },
    ]
};

const TARGET_HOST = __ENV.TARGET_HOST || '127.0.0.1';

export default function () {
    const url = `ws://${TARGET_HOST}:8080/ws`;
    
    ws.connect(url, function (socket) {
        socket.on('open', () => {
            socket.setInterval(function timeout() {
                socket.send('hello, extreme low latency!');
            }, 1000);
        });

        socket.on('message', (msg) => {
            check(msg, { 'is correct': (r) => r === 'hello, extreme low latency!' });
        });

        socket.setTimeout(function () {
            socket.close();
        }, 30000); 
    });
}