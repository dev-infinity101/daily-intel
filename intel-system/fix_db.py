import asyncio
from sqlalchemy import select, delete
from app.database import SessionLocal
from app.models.job import Job
from app.models.processed_item import ProcessedItem
from app.models.raw_item import RawItem

async def main():
    async with SessionLocal() as db:
        # 1. Delete dummy jobs (like '%Scrape%')
        stmt = select(Job).where(Job.company.ilike('%Scrape%'))
        res = await db.execute(stmt)
        jobs = res.scalars().all()
        for j in jobs:
            print(f"Found dummy job: {j.id} - {j.company} - {j.title}")
            await db.delete(j)
        
        # 2. Let's also check ProcessedItem for Scrape Test Co
        stmt = select(ProcessedItem).where(ProcessedItem.summary.ilike('%Scrape Test Co%'))
        res = await db.execute(stmt)
        pis = res.scalars().all()
        for p in pis:
            print(f"Found dummy processed item: {p.id}")
            await db.delete(p)
            
        await db.commit()
        print(f"Deleted {len(jobs)} jobs and {len(pis)} processed items.")

if __name__ == '__main__':
    asyncio.run(main())
