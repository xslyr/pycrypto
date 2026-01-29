from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_as_dataclass, mapped_column, registry

vector_registry = registry()


@mapped_as_dataclass(vector_registry)
class Documento:
    __tablename__ = "knowledge_base"

    id: Mapped[int] = mapped_column(primary_key=True)
    content: Mapped[str]
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB)
    embedding: Mapped[list[float]] = mapped_column(Vector(768))
