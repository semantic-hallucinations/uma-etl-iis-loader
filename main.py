import asyncio
import sys
from loguru import logger
from iis_etl.database import engine, get_session
from iis_etl.models import Base
from iis_etl.client import BsuirApiClient
from iis_etl.logic import (
    sync_system_state,
    sync_faculties, 
    sync_departments, 
    sync_specialities, 
    sync_groups, 
    sync_employees, 
    sync_auditories,
    sync_all_group_schedules,
    sync_all_employee_schedules,
    rebuild_occupancy_index
)


logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("etl_run.log", rotation="20 MB", level="DEBUG")


async def init_db():
    logger.info("Проверка схемы БД...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def main():
    logger.info("=== ЗАПУСК ETL JOB ===")
    
    await init_db()
    client = BsuirApiClient()
    
    async for session in get_session():
        try:
            await sync_system_state(session, client)

            await sync_faculties(session, client)
            await sync_departments(session, client)
            await sync_specialities(session, client)
            await sync_groups(session, client)
            await sync_employees(session, client)
            await sync_auditories(session, client)
            
            logger.success("Фаза 1 (Справочники) завершена.")
            
            await sync_all_group_schedules(session, client)
            await sync_all_employee_schedules(session, client)
            logger.success("Фаза 2 (Расписания) завершена.")

            await rebuild_occupancy_index(session)
            
            logger.success("=== ETL JOB ЗАВЕРШЕН УСПЕШНО ===")

        except Exception as e:
            logger.critical(f"FATAL ERROR: {e}")
            raise 
        finally:
            await client.close()
            break 


if __name__ == "__main__":
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.warning("Остановка пользователем")