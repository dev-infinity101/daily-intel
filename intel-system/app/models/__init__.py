# Import all models so Base.metadata is populated for Alembic and relationship resolution
from app.models.source import Source
from app.models.raw_item import RawItem
from app.models.processed_item import ProcessedItem
from app.models.digest import Digest
from app.models.target_company import TargetCompany
from app.models.job import Job

__all__ = ["Source", "RawItem", "ProcessedItem", "Digest", "TargetCompany", "Job"]
