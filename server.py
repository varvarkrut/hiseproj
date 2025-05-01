import datetime
import logging
import os
import time
import asyncio
import random
from typing import List, Optional, AsyncGenerator
from dotenv import load_dotenv


import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    BigInteger,
    UniqueConstraint,
    text,
    select,
    update,
    insert,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from contextlib import asynccontextmanager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

load_dotenv()
DB_USER = os.environ.get("DB_USER", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_HOST = os.environ.get("DB_HOST", "")
DB_PORT = os.environ.get("DB_PORT", "")
DB_NAME = os.environ.get("DB_NAME", "")
DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

db: Optional[async_sessionmaker[AsyncSession]] = None
Base = declarative_base()


class MessageCounter(Base):
    """
    Модель счетчика сообщений.
    """
    __tablename__ = "message_counters"

    counter_name = Column(String, primary_key=True)
    current_value = Column(BigInteger, nullable=False, default=0)


class Message(Base):
    """
    Модель сообщения.
    """
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    sender = Column(String, nullable=False)
    text = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    message_number = Column(BigInteger, nullable=False, unique=True)
    user_message_count = Column(BigInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint("sender", "user_message_count", name="unique_sender_count"),
    )


async def init_db(engine: AsyncEngine) -> None:
    """
    Функция для создания таблиц в базе данных.
    
    :param engine: Движок асинхронной базы данных SQLAlchemy
    :type engine: AsyncEngine
    :return: None
    """
    async with engine.begin() as conn:
        try:
            await conn.run_sync(Base.metadata.create_all)

            global_counter_exists = await conn.execute(
                select(MessageCounter).filter_by(counter_name="global_message_counter")
            )

            if not global_counter_exists.scalar_one_or_none():
                await conn.execute(
                    insert(MessageCounter).values(
                        counter_name="global_message_counter", current_value=0
                    )
                )
        except IntegrityError as e:
            logger.warning(
                f"Таблица уже существует или создается другой репликой: {str(e)}. "
                f"Ожидаемое нормальное поведение при запуске нескольких реплик."
            )


class MessageRequest(BaseModel):
    """
    Модель запроса для создания нового сообщения.
    """
    sender: str
    text: str


class MessageResponse(BaseModel):
    """
    Модель ответа с информацией о сообщении.
    """
    sender: str
    text: str
    timestamp: str
    message_number: int
    user_message_count: int

    class Config:
        orm_mode = True


async def increase_counter(db: AsyncSession, counter_name: str) -> int:
    """
    Функция для увеличения значения счетчика
    
    :param db: Сессия базы данных SQLAlchemy
    :type db: AsyncSession
    :param counter_name: Имя счетчика
    :type counter_name: str
    :return: Новое значение счетчика
    :rtype: int
    """
    counter = await db.execute(
        select(MessageCounter).filter_by(counter_name=counter_name).with_for_update()
    )
    counter_record = counter.scalar_one_or_none()

    new_value = counter_record.current_value + 1
    logger.info(
        f"Увеличиваем счетчик {counter_name} "
        f"со значения {counter_record.current_value} на {new_value}"
    )

    await db.execute(
        update(MessageCounter)
        .where(MessageCounter.counter_name == counter_name)
        .values(current_value=new_value)
    )
    await db.flush()

    return new_value


async def check_or_create_user_counter(db: AsyncSession, sender: str) -> str:
    """
    Функция для проверки существования счетчика пользователя и создания, если его нет
    
    :param db: Сессия базы данных SQLAlchemy
    :type db: AsyncSession
    :param sender: Имя отправителя сообщения
    :type sender: str
    :return: Имя счетчика пользователя
    :rtype: str
    """
    counter_name = f"user_{sender.lower().replace(' ', '_')}_counter"

    counter = await db.execute(
        select(MessageCounter).filter_by(counter_name=counter_name)
    )
    if not counter.scalar_one_or_none():
        counter_record = MessageCounter(counter_name=counter_name, current_value=0)
        db.add(counter_record)
        await db.flush()

    return counter_name


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Функция для получения сессии базы данных.
    
    :return: Асинхронная сессия базы данных
    :rtype: AsyncGenerator[AsyncSession, None]
    """
    session = db()
    yield session
    await session.close()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Функция для инициализации соединения с базой данных.
    
    :param app: Экземпляр приложения FastAPI
    :type app: FastAPI
    :return: Асинхронный генератор
    :rtype: AsyncGenerator[None, None]
    """
    engine = create_async_engine(
        DATABASE_URL, echo=False, isolation_level="REPEATABLE READ"
    )
    global db
    db = async_sessionmaker(engine)
    await init_db(engine=engine)
    yield
    await engine.dispose()


app = FastAPI(lifespan=lifespan)


@app.post("/message", response_model=List[MessageResponse])
async def post_message(
    message: MessageRequest, db: AsyncSession = Depends(get_db_session)
) -> List[MessageResponse]:
    """
    Функция для обработки нового сообщения
    
    :param message: Данные нового сообщения
    :type message: MessageRequest
    :param db: Сессия базы данных SQLAlchemy
    :type db: AsyncSession
    :return: Список последних 10 сообщений, включая новое
    :rtype: List[MessageResponse]
    :raises: HTTPException: В случае ошибки базы данных или таймаута
    """
    max_retries = 5
    timeout = 10
    retry_count = 0
    start_time = time.time()

    while retry_count < max_retries:
        if time.time() - start_time > timeout:
            logger.error(f"Превышен таймаут {timeout} секунд при обработке сообщения")
            raise HTTPException(status_code=504, detail="Timeout exceeded")

        try:
            async with db.begin():
                logger.info(
                    f"Попытка {retry_count + 1}: "
                    f"Обрабатываю сообщение от пользователя {message.sender}"
                )

                logger.info("Блокирую таблицу счетчиков")
                await db.execute(
                    text("LOCK TABLE message_counters IN SHARE ROW EXCLUSIVE MODE")
                )
                message_number = await increase_counter(
                    db, "global_message_counter"
                )
                logger.info(f"номер сообщения: {message_number}")

                user_counter_name = await check_or_create_user_counter(db, message.sender)

                user_message_count = await increase_counter(db, user_counter_name)

                timestamp = datetime.datetime.utcnow()
                logger.info(
                    f"номер сообщения={message_number}, "
                    f"счетчик_пользователя={user_message_count}"
                )

                new_message = Message(
                    sender=message.sender,
                    text=message.text,
                    timestamp=timestamp,
                    message_number=message_number,
                    user_message_count=user_message_count,
                )

                db.add(new_message)
                await db.flush()
                logger.info(f"Сообщение сохранено, id {new_message.id}")

                query = (
                    select(Message)
                    .filter(Message.message_number <= message_number)
                    .order_by(Message.message_number.desc())
                    .limit(10)
                )

                result = await db.execute(query)
                latest_messages = result.scalars().all()

                response_messages: List[MessageResponse] = []
                for msg in sorted(latest_messages, key=lambda m: m.message_number):
                    response_messages.append(
                        MessageResponse(
                            sender=msg.sender,
                            text=msg.text,
                            timestamp=msg.timestamp.isoformat(),
                            message_number=msg.message_number,
                            user_message_count=msg.user_message_count,
                        )
                    )
                await db.commit()
                logger.info("Транзакция успешно завершена")
                return response_messages

        except SQLAlchemyError as e:
            retry_count += 1
            logger.warning(
                f"Ошибка базы данных: {str(e)}, попытка {retry_count}/{max_retries}"
            )

            if retry_count >= max_retries or time.time() - start_time > timeout:
                logger.error(
                    f"Исчерпаны все попытки ({max_retries}) "
                    f"или превышен таймаут. Ошибка: {str(e)}"
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Не удалось обработать сообщение после {max_retries} попыток",
                )

            backoff = 0.1 * (2**retry_count)
            jitter = random.uniform(0, 0.1)
            wait_time = min(backoff + jitter, 1.0)

            logger.info(f"Ожидание {wait_time:.2f} секунд перед повторной попыткой")
            await asyncio.sleep(wait_time)

        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}"
            )


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
