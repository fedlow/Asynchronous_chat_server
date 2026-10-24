# Async Chat Server
Асинхронный чат на asyncio. Стандартная библиотека, без зависимостей.

## Запуск
- Сервер: `python server.py` (порт 8888)
- Клиент: `python client.py` (или `python client.py <host> <port>`)

## Команды
`/help`, `/users`, `/nick <name>`, `/me <action>`, `/file <path>`, `/quit`

## Что реализовано
- Многопользовательский чат с broadcast
- Имя + время в сообщениях
- Уведомления о входе/выходе
- Команды чата
- Передача файлов (base64)
- Логирование в `chat.log`