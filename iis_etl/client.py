import httpx
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .config import settings

class BsuirApiClient:
    def __init__(self):
        self.base_url = settings.API_BASE_URL.rstrip("/")
        self.semaphore = asyncio.Semaphore(settings.CONCURRENCY_LIMIT)
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json"
        }
        
        self.client = httpx.AsyncClient(
            timeout=120.0, 
            headers=headers,
            follow_redirects=True
        )

    async def close(self):
        await self.client.aclose()

    @retry(
        stop=stop_after_attempt(5), 
        wait=wait_exponential(multiplier=1, min=2, max=20),
        retry=retry_if_exception_type((httpx.RequestError, httpx.TimeoutException, httpx.HTTPStatusError))
    )
    async def _get(self, endpoint: str, params: dict = None):
        async with self.semaphore:
            url = f"{self.base_url}{endpoint}"
            response = await self.client.get(url, params=params)
            
            response.raise_for_status()
            return response.json()
    
    async def get_faculties(self):
        return await self._get("/faculties")

    async def get_departments(self):
        return await self._get("/departments")

    async def get_specialities(self):
        return await self._get("/specialities")
        
    async def get_student_groups(self):
        return await self._get("/student-groups")

    async def get_employees(self):
        return await self._get("/employees/all")

    async def get_auditories(self):
        return await self._get("/auditories")
    
    async def get_current_week(self):
        return await self._get("/schedule/current-week")
        
    async def get_group_schedule(self, group_name: str):
        return await self._get("/schedule", params={"studentGroup": group_name})

    async def get_employee_schedule(self, url_id: str):
        return await self._get(f"/employees/schedule/{url_id}")