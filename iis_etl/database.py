from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from .config import settings


engine = create_async_engine(settings.DB_DSN, echo=False)


AsyncSessionLocal = async_sessionmaker(
    bind=engine, 
    expire_on_commit=False, 
    class_=AsyncSession
)


async def get_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session