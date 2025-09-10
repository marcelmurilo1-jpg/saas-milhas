# backend/app/models.py
from sqlalchemy import Column, Integer, Text, Date, Boolean, JSON, TIMESTAMP
from .db import Base

class Promotion(Base):
    __tablename__ = "promocoes"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(Text, unique=True, nullable=False)
    title = Column(Text)
    date_published = Column(Date)
    author = Column(Text)
    content_text = Column(Text)
    content_html = Column(Text)
    images_json = Column(JSON)
    links_json = Column(JSON)
    scraped_at = Column(TIMESTAMP(timezone=True))
    valid_until = Column(TIMESTAMP(timezone=True))
    # se sua tabela tem expired/valid_candidates/tags inclua aqui também

    def to_dict(self):
        return {
            "id": self.id,
            "url": self.url,
            "title": self.title,
            "date_published": self.date_published.isoformat() if self.date_published else None,
            "author": self.author,
            "content_text": self.content_text,
            "content_html": self.content_html,
            "images_json": self.images_json,
            "links_json": self.links_json,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
            "valid_until": self.valid_until.isoformat() if self.valid_until else None
        }
