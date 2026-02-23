import asyncio
from telegram import Bot

from src.domain.ports import AbstractNotifications


class TelegramNotifications(AbstractNotifications):
    def __init__(self, bot_token: str):
        self.bot = Bot(token=bot_token)

    def send(self, destination, message):
        asyncio.run(self._send_async(destination, message))

    async def _send_async(self, destination, message):
        await self.bot.send_message(chat_id=destination, text=message)
