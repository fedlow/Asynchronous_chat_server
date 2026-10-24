import asyncio
import base64
import sys
from pathlib import Path


class ChatClient:
    def __init__(self, host: str = '127.0.0.1', port: int = 8888):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        print(f'[system] Connected to {self.host}:{self.port}')

    #Приём всего от сервера
    async def receive_loop(self):
        while True:
            try:
                data = await self.reader.readline()
            except Exception as e:
                print(f'[system] Read error: {e}')
                return
            if not data:
                print('\n[system] Server closed connection.')
                return
            text = data.decode().rstrip('\n')

            # Файл?
            if text.startswith('FILE|'):
                self._handle_file(text)
                continue

            print(text)

    def _handle_file(self, text: str):
        try:
            _, sender, filename, b64data = text.split('|', 3)
            raw = base64.b64decode(b64data)
            downloads = Path('downloads')
            downloads.mkdir(exist_ok=True)
            out = downloads / f'{sender}_{filename}'
            out.write_bytes(raw)
            print(f'[system] 📎 Saved file from {sender}: {out} ({len(raw)} bytes)')
        except Exception as e:
            print(f'[system] File receive error: {e}')

    #Отправка
    async def send_loop(self):
        while True:
            try:
                line = await asyncio.to_thread(sys.stdin.readline)
            except Exception:
                return
            if not line:
                return
            line = line.rstrip('\n')
            if not line:
                continue

            # /file filename
            if line.startswith('/file '):
                path_str = line[len('/file '):].strip()
                p = Path(path_str)
                if not p.is_file():
                    print(f'[local] File not found: {p}')
                    continue
                try:
                    raw = p.read_bytes()
                    b64 = base64.b64encode(raw).decode()
                    await self._send(f'/file {p.name} {b64}')
                    print(f'[local] Sent file {p.name} ({len(raw)} bytes)')
                except Exception as e:
                    print(f'[local] Error reading file: {e}')
                continue

            # /quit
            if line.strip() == '/quit':
                await self._send(line)
                return

            await self._send(line)

    async def _send(self, text: str):
        try:
            self.writer.write((text + '\n').encode())
            await self.writer.drain()
        except Exception as e:
            print(f'[system] Send error: {e}')

    #Основной цикл
    async def run(self):
        await self.connect()
        t_recv = asyncio.create_task(self.receive_loop())
        t_send = asyncio.create_task(self.send_loop())

        done, pending = await asyncio.wait(
            {t_recv, t_send}, return_when=asyncio.FIRST_COMPLETED
        )
        for p in pending:
            p.cancel()

        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass
        print('[system] Disconnected')


if __name__ == '__main__':
    host = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8888
    try:
        asyncio.run(ChatClient(host, port).run())
    except KeyboardInterrupt:
        print('\n[system] Interrupted')