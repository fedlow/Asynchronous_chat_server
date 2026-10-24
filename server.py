import asyncio
import base64
import logging
from datetime import datetime
from pathlib import Path

# ЛОГИРОВАНИЕ
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('chat.log', encoding='utf-8'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger('ChatServer')


# КЛИЕНТ (обёртка над reader/writer)
class Client:
    """Один подключённый пользователь."""
    def __init__(self, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter,
                 nickname: str):
        self.reader = reader
        self.writer = writer
        self.nickname = nickname

    async def send(self, text: str):
        """Отправить строку + \n."""
        self.writer.write((text + '\n').encode())
        await self.writer.drain()

# СЕРВЕР
class ChatServer:
    def __init__(self, host: str = '127.0.0.1', port: int = 8888):
        self.host = host
        self.port = port
        # nickname -> Client
        self.clients: dict[str, Client] = {}

    #Запуск
    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        addr = server.sockets[0].getsockname()
        logger.info(f'🚀 Chat server started on {addr[0]}:{addr[1]}')
        async with server:
            await server.serve_forever()

    #Обработка одного клиента
    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        client: Client | None = None

        try:
            # 1) Спрашиваем ник
            writer.write(b'Welcome! Please enter your nickname:\n')
            await writer.drain()

            raw = await asyncio.wait_for(reader.readline(), timeout=60)
            nickname = raw.decode().strip()[:30]

            # 2) Валидация ника
            if not nickname:
                writer.write(b'[system] Empty nickname, disconnect.\n')
                await writer.drain()
                return
            if nickname in self.clients:
                writer.write(f'[system] Nickname "{nickname}" is taken.\n'.encode())
                await writer.drain()
                return

            # 3) Регистрируем
            client = Client(reader, writer, nickname)
            self.clients[nickname] = client
            logger.info(f'➕ {nickname} connected from {addr}')

            # 4) Уведомляем всех остальных
            await self.broadcast(f'[system] {nickname} joined the chat.',
                                 exclude=nickname)
            await client.send(
                f'[system] Welcome, {nickname}! Users online: '
                f'{len(self.clients)}. Type /help for commands.'
            )

            # 5) Основной цикл чтения
            while True:
                data = await reader.readline()
                if not data:
                    break   # клиент закрыл соединение
                line = data.decode().rstrip('\n')
                if not line.strip():
                    continue

                if line.startswith('/'):
                    keep = await self.handle_command(client, line)
                    if not keep:
                        break
                else:
                    await self.handle_message(client, line)

        except asyncio.TimeoutError:
            logger.warning(f'⏱  {addr} timeout waiting for nickname')
        except (ConnectionResetError, BrokenPipeError):
            logger.warning(f'⚠  {addr} reset connection')
        except Exception as e:
            logger.exception(f'💥 Error on {addr}: {e}')
        finally:
            # Очистка и уведомление остальных
            if client and client.nickname in self.clients:
                del self.clients[client.nickname]
                logger.info(f'➖ {client.nickname} disconnected')
                await self.broadcast(
                    f'[system] {client.nickname} left the chat.',
                    exclude=None,
                )
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # ---------- Обычное сообщение (K3) ----------
    async def handle_message(self, client: Client, text: str):
        ts = datetime.now().strftime('%H:%M:%S')
        formatted = f'[{ts}] {client.nickname}: {text}'
        logger.info(f'💬 {client.nickname}: {text}')
        # шлём всем, кроме самого отправителя
        await self.broadcast(formatted, exclude=client.nickname)
        # отправитель видит своё сообщение с временем
        await client.send(formatted)

    #Команды
    async def handle_command(self, client: Client, line: str) -> bool:
        """Возвращает False, если клиента нужно отключить."""
        parts = line.split(maxsplit=2)
        cmd = parts[0].lower()
        args = parts[1:]

        # /help
        if cmd == '/help':
            await client.send(
                'Commands:\n'
                '  /help                — this help\n'
                '  /users               — list online users\n'
                '  /nick <new_name>     — change nickname\n'
                '  /me <action>         — action message\n'
                '  /file <name> <b64>   — share a file (base64)\n'
                '  /quit                — disconnect'
            )

        # /users
        elif cmd == '/users':
            users = ', '.join(self.clients.keys())
            await client.send(f'[system] Online ({len(self.clients)}): {users}')

        # /nick
        elif cmd == '/nick':
            if not args:
                await client.send('[system] Usage: /nick <new_name>')
                return True
            new_name = args[0].strip()[:30]
            if not new_name or new_name in self.clients:
                await client.send('[system] Invalid or taken nickname')
                return True
            old_name = client.nickname
            del self.clients[old_name]
            client.nickname = new_name
            self.clients[new_name] = client
            logger.info(f'✏  {old_name} renamed to {new_name}')
            await self.broadcast(
                f'[system] {old_name} is now known as {new_name}.',
                exclude=None,
            )

        # /me
        elif cmd == '/me':
            action = ' '.join(args).strip()
            if not action:
                await client.send('[system] Usage: /me <action>')
                return True
            ts = datetime.now().strftime('%H:%M:%S')
            await self.broadcast(f'[{ts}] * {client.nickname} {action}',
                                 exclude=None)

        # /file
        elif cmd == '/file':
            if len(args) < 2:
                await client.send('[system] Usage: /file <filename> <base64>')
                return True
            filename = args[0].strip()
            b64data = args[1].strip()
            try:
                raw = base64.b64decode(b64data, validate=True)
            except Exception as e:
                await client.send(f'[system] Base64 decode error: {e}')
                return True
            size = len(raw)
            logger.info(f'📎 {client.nickname} shared file "{filename}" ({size} bytes)')
            # Рассылаем всем в формате FILE|отправитель|имя|base64
            bcast = f'FILE|{client.nickname}|{filename}|{b64data}'
            await self.broadcast(bcast, exclude=None)

        # /quit
        elif cmd == '/quit':
            await client.send('[system] Bye!')
            return False

        # неизвестная команда
        else:
            await client.send(f'[system] Unknown command: {cmd}. Try /help')

        return True

    #Рассылка
    async def broadcast(self, message: str, exclude: str | None = None):
        """Шлёт строку всем, кроме exclude (по нику). Мёртвых клиентов убирает."""
        data = message + '\n'
        dead: list[str] = []
        for nick, c in list(self.clients.items()):
            if nick == exclude:
                continue
            try:
                await c.send(data)
            except Exception:
                dead.append(nick)
        for nick in dead:
            self.clients.pop(nick, None)
            logger.warning(f'💀 Removed dead client: {nick}')


# ТОЧКА ВХОДА
if __name__ == '__main__':
    server = ChatServer(host='127.0.0.1', port=8888)
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('🛑 Server stopped by user')